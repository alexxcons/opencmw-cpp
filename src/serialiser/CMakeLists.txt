# setup header only library
add_library(serialiser INTERFACE include/IoSerialiserYAML.hpp)
target_include_directories(serialiser INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include/>)
target_link_libraries(serialiser INTERFACE $<BUILD_INTERFACE: mp-units::mp-units mustache::mustache> refl-cpp::refl-cpp core utils)
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
