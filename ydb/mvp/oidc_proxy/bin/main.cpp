#include <ydb/mvp/oidc_proxy/mvp.h>

int main(int argc, char **argv) {
    try {
        return NMVP::NOIDC::TMVP(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
