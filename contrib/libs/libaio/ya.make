LIBRARY()

# git repository: https://pagure.io/libaio.git
# revision: 5a546a834c36070648158d19dd564762d59f8eb8

LICENSE(Service-Dll-Harness) 

WITHOUT_LICENSE_TEXTS() 

VERSION(2015-07-01-5a546a834c36070648158d19dd564762d59f8eb8)

OWNER(
    g:contrib
    g:cpp-contrib
)

NO_RUNTIME()

IF (USE_DYNAMIC_AIO)
    PEERDIR(
        contrib/libs/libaio/dynamic
    )
ELSE()
    PEERDIR(
        contrib/libs/libaio/static
    )
ENDIF()

END()

RECURSE(
    dynamic
    static
)
