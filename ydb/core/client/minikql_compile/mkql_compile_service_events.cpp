#include "mkql_compile_service.h"

namespace NKikimr {

TMiniKQLCompileServiceEvents::TEvCompile::TEvCompile(const TString& program)
    : Program(program)
{}

TMiniKQLCompileServiceEvents::TEvCompileStatus::TEvCompileStatus(const TString& pgm, const NYql::TMiniKQLCompileResult& result)
    : Program(pgm)
    , Result(result)
{}

NActors::TActorId MakeMiniKQLCompileServiceID() {
    const char x[12] = "MKQLCompile";
    return NActors::TActorId(0, TStringBuf(x, 12));
}

const NActors::TActorId& GetMiniKQLCompileServiceID() {
    static NActors::TActorId miniKQLCompileServiceID = MakeMiniKQLCompileServiceID();
    return miniKQLCompileServiceID;
}

} // namespace NKikimr
