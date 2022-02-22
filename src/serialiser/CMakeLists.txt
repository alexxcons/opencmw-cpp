# setup header only library
cmrc_add_resource_library(
        MUSTACHE_TEST_SERVER_ASSETS ALIAS assets::mustache NAMESPACE assets
        ${CMAKE_CURRENT_SOURCE_DIR}/test/assets/mustache/default.mustache
        ${CMAKE_CURRENT_SOURCE_DIR}/test/assets/mustache/Services.mustache
)

add_library(serialiser INTERFACE include/IoSerialiserYAML.hpp)
target_include_directories(serialiser INTERFACE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/serialiser/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include/>)
target_link_libraries(serialiser INTERFACE CONAN_PKG::mp-units refl-cpp::refl-cpp utils core $<BUILD_INTERFACE:assets::mustache>)
set_target_properties(serialiser PROPERTIES PUBLIC_HEADER "include/fast_float.h;include/IoBuffer.hpp;include/IoSerialiserYaS.hpp;include/IoSerialiserJson.hpp;include/IoSerialiserCmwLight.hpp;include/IoSerialiser.hpp;include/MultiArray.hpp;include/opencmw.hpp")

install(
        TARGETS serialiser
        EXPORT opencmwTargets
        PUBLIC_HEADER DESTINATION include/opencmw
)

# setup tests
if(ENABLE_TESTING)
  add_subdirectory(test)
endif()
