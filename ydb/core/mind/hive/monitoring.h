#include <library/cpp/json/json_writer.h>
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/mon.h>

namespace NKikimr {
namespace NHive {

class TLoggedMonTransaction {
private:
    ui64 Index;
    TString User;

protected:
    TLoggedMonTransaction(const NMon::TEvRemoteHttpInfo::TPtr& evi, THive* self);

    void WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op);
};

}
}
