#include "schemeshard_impl.h"

#include <ydb/library/login/hashes_checker/hashes_checker.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/login/protos/login.pb.h>

#include <util/generic/vector.h>
#include <util/random/random.h>
#include <algorithm>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

namespace {

TString GenerateRandomPassword(const NLogin::TPasswordComplexity& complexity) {
    static constexpr char lowerLetters[] = "abcdefghijklmnopqrstuvwxyz";
    static constexpr char upperLetters[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static constexpr char digits[] = "0123456789";
    static constexpr size_t lowerCount = sizeof(lowerLetters) - 1;
    static constexpr size_t upperCount = sizeof(upperLetters) - 1;
    static constexpr size_t digitsCount = sizeof(digits) - 1;


    TVector<char> specialChars(complexity.SpecialChars.begin(), complexity.SpecialChars.end());

    TVector<char> password;

    // Add required minimum counts for each character class
    for (size_t i = 0; i < complexity.MinLowerCaseCount; ++i) {
        password.push_back(lowerLetters[RandomNumber<size_t>(lowerCount)]);
    }
    for (size_t i = 0; i < complexity.MinUpperCaseCount; ++i) {
        password.push_back(upperLetters[RandomNumber<size_t>(upperCount)]);
    }
    for (size_t i = 0; i < complexity.MinNumbersCount; ++i) {
        password.push_back(digits[RandomNumber<size_t>(digitsCount)]);
    }
    for (size_t i = 0; i < complexity.MinSpecialCharsCount; ++i) {
        password.push_back(specialChars[RandomNumber<size_t>(specialChars.size())]);
    }

    size_t minLength = std::max(complexity.MinLength, static_cast<size_t>(8));
    // Fill remaining positions with random chars from all allowed categories
    if (password.size() < minLength) {

        TVector<char> pool;
        pool.insert(pool.end(), lowerLetters, lowerLetters + lowerCount);
        pool.insert(pool.end(), upperLetters, upperLetters + upperCount);
        pool.insert(pool.end(), digits, digits + digitsCount);
        pool.insert(pool.end(), specialChars.begin(), specialChars.end());

        while (password.size() < minLength) {
            password.push_back(pool[RandomNumber<size_t>(pool.size())]);
        }
    }


    // Shuffle to avoid predictable prefix pattern
    for (size_t i = password.size() - 1; i > 0; --i) {
        size_t j = RandomNumber<size_t>(i + 1);
        std::swap(password[i], password[j]);
    }

    return TString(password.begin(), password.end());
}

} // anonymous namespace

struct TSchemeShard::TTxUserHashesMigration : public TTransactionBase<TSchemeShard> {
    bool IsLoginProviderModified = false;

    TTxUserHashesMigration(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_USER_HASHES_MIGRATION;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxUserHashesMigration Execute at schemeshard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [sidName, sid] : Self->LoginProvider.Sids) {
            if (sid.Type == NLoginProto::ESidType::USER) {
                if (!sid.PasswordHashes) { // change password for user with unsupported password hash format
                    auto response = Self->LoginProvider.ModifyUser({
                        .User = sid.Name,
                        .Password = GenerateRandomPassword(Self->LoginProvider.GetPasswordCheckParameters()),
                    });

                    if (response.Error) {
                        LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxUserHashesMigration Execute"
                            << ", can't set generated password in place unacceptable argon hash: "
                            << response.Error << ", at schemeshard: "<< Self->TabletID());
                        continue;
                    }

                    IsLoginProviderModified = true;
                }

                // persist password hashes in the new column 'PasswordHashes' to get rid of old column 'SidHash' in the next release
                db.Table<Schema::LoginSids>().Key(sidName).Update<Schema::LoginSids::SidHash, Schema::LoginSids::PasswordHashes>(
                                                                    sid.ArgonHash, sid.PasswordHashes);
            }
        }

        Self->IsOldArgonHashFormatMigrationCompleted = true;
        db.Table<Schema::SysParams>().Key(Schema::SysParam_IsOldArgonHashFormatMigrationCompleted).Update(
            NIceDb::TUpdate<Schema::SysParams::Value>("1"));

        if (IsLoginProviderModified) {
            TPathId subDomainPathId = Self->GetCurrentSubDomainPathId();
            TSubDomainInfo::TPtr domainPtr = Self->ResolveDomainInfo(subDomainPathId);
            domainPtr->UpdateSecurityState(Self->LoginProvider.GetSecurityState());
            domainPtr->IncSecurityStateVersion();
            Self->PersistSubDomainSecurityStateVersion(db, subDomainPathId, *domainPtr);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxUserHashesMigration Complete, at schemeshard: "<< Self->TabletID());

        if (IsLoginProviderModified) {
            Self->PublishToSchemeBoard(TTxId(), {Self->GetCurrentSubDomainPathId()}, ctx);
        }
    }
};

ITransaction* TSchemeShard::CreateTxUserHashesMigration() {
    return new TTxUserHashesMigration(this);
}

} // NSchemeShard
} // NKikimr
