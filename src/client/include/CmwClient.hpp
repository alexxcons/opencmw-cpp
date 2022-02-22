#ifndef OPENCMW_CPP_CMW_CLIENT_HPP
#define OPENCMW_CPP_CMW_CLIENT_HPP

#include <charconv>
#include <opencmw.hpp>
#include <majordomo/ZmqPtr.hpp> // from majordomo project, consider to move to core or util to make client independent of majodomo (but core dep on zmq :/)
#include <majordomo/Settings.hpp> // from majordomo project, consider to move to core or util to make client independent of majodomo (but core dep on zmq :/)
#include <majordomo/Utils.hpp>
#include <majordomo/Message.hpp>
#include <URI.hpp>
#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <chrono>
#include <utility>
#include <disruptor/Disruptor.hpp>
#include <IoSerialiser.hpp>

namespace opencmw::client {

using namespace std::literals;

template<ReflectableClass T>
class CmwClient{
private:
    static std::atomic<int> _instanceCount;
    const disruptor::RingBuffer<T> _ringBuffer;
    const std::chrono::milliseconds _clientTimeout;
    const majordomo::Context &_context;
    std::optional<majordomo::Socket> _socket;
    const majordomo::Socket _controlSocket;
    const majordomo::Socket _monitorSocket;
    const URI<uri_check::RELAXED> _endpoint;
    const URI<uri_check::RELAXED> _server_uri;
    const std::string _clientId;
    const std::string _sourceName;
    std::chrono::milliseconds _heartbeatInterval;
    zmq_pollitem_t _pollItem;
    long _nextReconnectAttemptTimeStamp;
    long _nextRequestId = 0;
    // executor service

public:
    struct Request {
        int id;
    };

    template<typename BodyType>
    Request get(const std::string_view &serviceName, BodyType request) {
        auto [handle, message] = createRequestTemplate(majordomo::Command::Get, serviceName);
        message.setBody(std::forward<BodyType>(request), majordomo::MessageFrame::dynamic_bytes_tag{});
        message.send(*_socket).assertSuccess();
        return handle;
    }

    template<typename BodyType, typename Callback>
    Request get(const std::string_view &serviceName, BodyType request, Callback fnc) {
        auto r = get(serviceName, std::forward<BodyType>(request));
        //_callbacks.emplace(r.id, std::move(fnc));
        return r;
    }

    template<typename BodyType>
    Request subscribe(const std::string_view &serviceName, BodyType request) {
        auto [handle, message] = createRequestTemplate(majordomo::Command::Subscribe, serviceName);
        message.setBody(std::forward<BodyType>(request), majordomo::MessageFrame::dynamic_bytes_tag{});
        message.send(*_socket).assertSuccess();
        return handle;
    }

    template<typename BodyType, typename Callback>
    Request subscribe(const std::string_view &serviceName, BodyType request, Callback fnc) {
        auto r = subscribe(serviceName, std::forward<BodyType>(request));
        //_callbacks.emplace(r.id, std::move(fnc));
        return r;
    }

    URI<RELAXED> connect() {
        const auto result = zmq_invoke(zmq_connect, *_socket, majordomo::toZeroMQEndpoint(URI<STRICT>(_endpoint.str)).data());
        if (result.isValid()) {
            _pollItem.socket = _socket->zmq_ptr;
            _pollItem.events = ZMQ_POLLIN;
        } else {
            _socket.reset();
        }
        return _endpoint;
    }

    bool disconnect() {
        _socket.reset();
        return true;
    }

    bool handleMessage(majordomo::MdpMessage &&message) {
        if (!message.isValid()) {
            return false;
        }

        const auto idStr = message.clientRequestId();
        int        id;
        auto       asInt   = std::from_chars(idStr.begin(), idStr.end(), id);

        bool       handled = false;
        // handle get/set request callbacks
        // if (asInt.ec != std::errc::invalid_argument) {
        //     auto it = _callbacks.find(id);
        //     if (it != _callbacks.end()) {
        //         auto [id_key, callback] = it.get();
        //         handled = true;
        //         callback(std::move(message));
        //         _callbacks.erase(it);
        //     }
        // }

        // publish subscription updates to ring buffer
        if (!handled && message.command() == majordomo::Command::Notify) {
            //handleResponse(std::move(message));
            auto buffer = IoBuffer();
            buffer.resize(message.body().size()); // todo: zero-copy?
            std::copy(message.body().begin(), message.body().end(), buffer.data());
            _ringBuffer.publishEvent([&buffer](auto seq, T& ev) {
                deserialise<T>(buffer, ev);
            });
        }

        return handled;
    }

    // method to be called in regular time intervals to send and verify heartbeats
    std::chrono::milliseconds handleHeartbeats() {
        const auto result = majordomo::zmq_invoke(zmq_poll, &_pollItem, 1, _clientTimeout.count());
        if (!result.isValid()) {
            return _heartbeatInterval/10;
        }
        // TODO handle client HEARTBEAT etc.
        if (auto message = majordomo::MdpMessage::receive(*_socket)) {
            return handleMessage(std::move(*message));
        }

        return 0ms;
    }

    template <uri_check check>
    CmwClient(const majordomo::Context &context, const URI<check> &endpoint, const std::chrono::milliseconds timeout, /*executorService,*/ std::string clientId = "") :
            _clientTimeout(timeout),
            _context(context),
            _controlSocket(context, ZMQ_PAIR),
            _monitorSocket{context, ZMQ_PAIR},
            _endpoint(endpoint),
            _server_uri(fmt::format("{}://{}/", endpoint.scheme(), endpoint.authority())),
            _clientId(std::move(clientId)),
            _sourceName(fmt::format("OpenCmwClient(ID: {}, endpoint: {}, clientId: {})", _instanceCount++, _endpoint, _clientId)) {
        auto commonSocketInit = [](majordomo::Socket &socket) {
            const majordomo::Settings settings_{};
            const int heartbeatInterval = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(settings_.heartbeatInterval).count());
            const int ttl               = heartbeatInterval * settings_.heartbeatLiveness;
            const int hb_timeout           = heartbeatInterval * settings_.heartbeatLiveness;
            return zmq_invoke(zmq_setsockopt, socket, ZMQ_SNDHWM, &settings_.highWaterMark, sizeof(settings_.highWaterMark))
                   && zmq_invoke(zmq_setsockopt, socket, ZMQ_RCVHWM, &settings_.highWaterMark, sizeof(settings_.highWaterMark))
                   && zmq_invoke(zmq_setsockopt, socket, ZMQ_HEARTBEAT_TTL, &ttl, sizeof(ttl))
                   && zmq_invoke(zmq_setsockopt, socket, ZMQ_HEARTBEAT_TIMEOUT, &hb_timeout, sizeof(hb_timeout))
                   && zmq_invoke(zmq_setsockopt, socket, ZMQ_HEARTBEAT_IVL, &heartbeatInterval, sizeof(heartbeatInterval))
                   && zmq_invoke(zmq_setsockopt, socket, ZMQ_LINGER, &heartbeatInterval, sizeof(heartbeatInterval));
        };
        // create socket
        std::string scheme = endpoint.scheme().value_or("unknown");
        std::transform(scheme.begin(), scheme.end(), scheme.begin(), ::tolower);
        if (scheme == "mdp"sv) {
            _socket.emplace(_context, ZMQ_DEALER);
        } else if (scheme == "mds"sv) {
            _socket.emplace(_context, ZMQ_SUB);
        } else if (scheme == "mdr"sv) {
            throw ProtocolException("RADIO-DISH pattern is not yet implemented"); // _socket.emplace(_context, ZMQ_DISH)
        } else {
            throw ProtocolException("Unsupported protocol type " + endpoint.scheme().value_or("unknown")); // can only be reached if someone fiddles with the factory method
        }
        commonSocketInit(_socket.value()).assertSuccess();

        // create socket monitor
        // auto rc = zmq_socket_monitor(_socket.zmq_ptr, "inproc://monitor-client", ZMQ_EVENT_CLOSED | ZMQ_EVENT_CONNECTED | ZMQ_EVENT_DISCONNECTED);
        // assert(rc == 0);
        // majordomo::zmq_invoke(zmq_connect, _monitorSocket, "inproc://monitor-client").assertSuccess();

        // take the minimum of the (albeit worker) heartbeat, client (if defined) or locally prescribed timeout
        std::chrono::milliseconds HEARTBEAT_INTERVAL(1000);
        _heartbeatInterval = std::chrono::milliseconds(std::min(HEARTBEAT_INTERVAL.count(), _clientTimeout == std::chrono::milliseconds(0) ? timeout.count() : std::min(timeout.count(), _clientTimeout.count())));

        _nextReconnectAttemptTimeStamp = std::chrono::system_clock::now().time_since_epoch().count() + timeout.count();
        URI reply = connect();
        if (reply.str == "") {
            fmt::print("could not connect URI {} immediately - source {}", _endpoint, _sourceName);
        }
    }

    // reconnect
    // close
    // getMessage
    // housekeeping
    // subscribe
    // unsubscribe
    // get
    // set
    // toString
    // getFactory
    // handleRequest
    // createInternalMsg
    // INSTANCE_COUNT
    // MDP
    // MDS
    // MDR
    // APPLICABLE_SCHEMES
    // RESOLVERS
    // FACTORY
    // connectionState
    // reconnectAttempt
    // sourceName
    // timeout
    // executorService
    // clientId
    // endpoint
    // context
    // socket
    // socketMonitor
    // subscriptions
    // serverUri
    // subscriptionMatcher
    // heartbeatInterval
    // dnsWorkerResult
    // nextReconnectAttemptTimeStamp
    // connectedAddress

    // factory
    //   applicable schemes
    //   serialiser type
    //   get instance
    //   get dns/name resolver
private:
    std::pair<Request, majordomo::MdpMessage> createRequestTemplate(majordomo::Command command, std::string_view serviceName) {
        auto req = std::make_pair(makeRequestHandle(), majordomo::MdpMessage::createClientMessage(command));
        req.second.setServiceName(serviceName, majordomo::MessageFrame::dynamic_bytes_tag{});
        req.second.setClientRequestId(std::to_string(req.first.id), majordomo::MessageFrame::dynamic_bytes_tag{});
        return req;
    }

    Request makeRequestHandle() {
        return Request{ _nextRequestId++ };
    }
};

template <ReflectableClass T> // TODO: this should actually be global for all instances, find out how to do that or drop it
std::atomic<int> CmwClient<T>::_instanceCount{0};

} // namespace opencmw::client
#endif // OPENCMW_CPP_CMW_CLIENT_HPP
