#include <catch2/catch.hpp>
#include <fmt/format.h>
#include <CmwClient.hpp>
#include <majordomo/Broker.hpp>
#include <disruptor/Disruptor.hpp>
#include <majordomo/Worker.hpp>
#include "../../majordomo/test/helpers.hpp"

namespace cmwclienttest {
struct mystruct {
    int         a   = 1;
    std::string foo = "bar";
};
} // namescpace cmwclienttest // leave namespace for reflection macro
ENABLE_REFLECTION_FOR(cmwclienttest::mystruct, a, foo);

namespace cmwclienttest {

TEST_CASE("basic tests", "[CmwClient]") {
    using namespace opencmw;
    using namespace opencmw::client;
    using namespace opencmw::majordomo; // for ZmqPtr wrapper
    using namespace std::chrono_literals;

    CmwClient<mystruct> client(Context(), URI<uri_check::RELAXED>("mdp://service/device/property?filterA=false"), 1000ms, "clientID");

    REQUIRE_NOTHROW([&client]() {client.connect();});

}

TEST_CASE("Test subscription", "[client][subscription]") {
    using opencmw::majordomo::Broker;
    using opencmw::majordomo::MdpMessage;
    using namespace std::literals;

    Broker                   broker("testbroker", testSettings());

    opencmw::majordomo::BasicWorker<"beverages"> worker(broker, TestIntHandler(10));

    TestNode<opencmw::majordomo::BrokerMessage>  client(broker.context, ZMQ_XSUB);
    REQUIRE(client.connect(opencmw::majordomo::INTERNAL_ADDRESS_PUBLISHER));

    RunInThread brokerRun(broker);
    RunInThread workerRun(worker);

    // send some invalid subscribe/unsubscribe messages, must be ignored
    REQUIRE(client.sendRawFrame(""));
    REQUIRE(client.sendRawFrame("\x1"));
    REQUIRE(client.sendRawFrame("\x0"s));

    // subscribe to /wine* and /beer*
    REQUIRE(client.sendRawFrame("\x1/wine*"));
    REQUIRE(client.sendRawFrame("\x1/beer*"));

    bool seenNotification = false;

    // we have a potential race here: the worker might not have processed the
    // subscribe yet and thus discarding the notification. Send notifications
    // in a loop until one gets through.
    while (!seenNotification) {
        {
            MdpMessage notify;
            notify.setTopic("/beer.time", static_tag);
            notify.setBody("Have a beer", static_tag);
            REQUIRE(worker.notify(std::move(notify)));
        }
        {
            const auto notification = client.tryReadOne(std::chrono::milliseconds(20));
            if (notification && notification->serviceName() != "mmi.service") {
                seenNotification = true;
                REQUIRE(notification->isValid());
                REQUIRE(notification->isClientMessage());
                REQUIRE(notification->command() == opencmw::majordomo::Command::Final);
                REQUIRE(notification->sourceId() == "/beer*");
                REQUIRE(notification->topic() == "/beer.time");
                REQUIRE(notification->body() == "Have a beer");
            }
        }
    }

    {
        MdpMessage notify;
        notify.setTopic("/beer.error", static_tag);
        notify.setError("Fridge empty!", static_tag);
        REQUIRE(worker.notify(std::move(notify)));
    }

    bool seenError = false;
    while (!seenError) {
        const auto notification = client.tryReadOne(std::chrono::milliseconds(20));
        if (!notification)
            continue;

        // there might be extra messages from above, ignore them
        if (notification->topic() == "/beer.time") {
            continue;
        }

        REQUIRE(notification->isValid());
        REQUIRE(notification->isClientMessage());
        REQUIRE(notification->command() == opencmw::majordomo::Command::Final);
        REQUIRE(notification->sourceId() == "/beer*");
        REQUIRE(notification->topic() == "/beer.error");
        REQUIRE(notification->error() == "Fridge empty!");
        seenError = true;
    }

    {
        // as the subscribe for wine* was sent before the beer* one, this should be
        // race-free now (as know the beer* subscribe was processed by everyone)
        MdpMessage notify;
        notify.setTopic("/wine.italian", static_tag);
        notify.setBody("Try our Chianti!", static_tag);
        REQUIRE(worker.notify(std::move(notify)));
    }

    {
        const auto notification = client.tryReadOne();
        REQUIRE(notification.has_value());
        REQUIRE(notification->isValid());
        REQUIRE(notification->isClientMessage());
        REQUIRE(notification->command() == opencmw::majordomo::Command::Final);
        REQUIRE(notification->sourceId() == "/wine*");
        REQUIRE(notification->topic() == "/wine.italian");
        REQUIRE(notification->body() == "Try our Chianti!");
    }

    // unsubscribe from /beer*
    REQUIRE(client.sendRawFrame("\x0/beer*"s));

    // loop until we get two consecutive messages about wine, it means that the beer unsubscribe was processed
    while (true) {
        {
            MdpMessage notify;
            notify.setTopic("/wine.portuguese", static_tag);
            notify.setBody("New Vinho Verde arrived.", static_tag);
            REQUIRE(worker.notify(std::move(notify)));
        }
        {
            MdpMessage notify;
            notify.setTopic("/beer.offer", static_tag);
            notify.setBody("Get our pilsner now!", static_tag);
            REQUIRE(worker.notify(std::move(notify)));
        }
        {
            MdpMessage notify;
            notify.setTopic("/wine.portuguese", static_tag);
            notify.setBody("New Vinho Verde arrived.", static_tag);
            REQUIRE(worker.notify(std::move(notify)));
        }

        const auto msg1 = client.tryReadOne();
        REQUIRE(msg1.has_value());
        REQUIRE(msg1->sourceId() == "/wine*");

        const auto msg2 = client.tryReadOne();
        REQUIRE(msg2.has_value());
        if (msg2->sourceId() == "/wine*") {
            break;
        }

        REQUIRE(msg2->sourceId() == "/beer*");

        const auto msg3 = client.tryReadOne();
        REQUIRE(msg3.has_value());
        REQUIRE(msg3->sourceId() == "/wine*");
    }
}

}
