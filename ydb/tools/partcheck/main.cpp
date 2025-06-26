#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/erasure/erasure.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <map>

using namespace NKikimr;

int main(int argc, char *argv[]) {
    if (argc < 3) {
        Cerr << "usage: " << argv[0] << " <erasure> <files>" << Endl;
        return 1;
    }

    std::optional<TLogoBlobID> current;

    const auto species = TBlobStorageGroupType::ErasureSpeciesByName(argv[1]);
    if (species == TBlobStorageGroupType::ErasureSpeciesCount) {
        Cerr << "invalid erasure species name" << Endl;
        return 1;
    }
    TBlobStorageGroupType gtype(species);

    std::vector<TRcBuf> parts;
    parts.resize(gtype.TotalPartCount());

    bool errors = false;

    auto finishBucket = [&] {
        TStringBuilder log;

        auto addLog = [&](TString msg) {
            if (!log) {
                log << current->ToString();
            }
            log << ' ' << msg;
        };

        if (gtype.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
            TRope expected;
            for (const auto& part : parts) {
                if (!part) {
                    continue;
                }
                if (!expected) {
                    expected = part;
                } else if (expected != part) {
                    addLog("part value incorrect");
                    errors = true;
                }
            }
        } else if (gtype.GetErasure() == TBlobStorageGroupType::ErasureMirror3of4) {
            if (parts[0] && parts[1] && parts[0] != parts[1]) {
                addLog("part value incorrect");
                errors = true;
            }
        } else {
            ui32 restoreMask = 0;
            std::vector<TRope> ropes(parts.size());
            for (size_t i = 0; i < parts.size(); ++i) {
                ropes[i] = parts[i];
                if (!ropes[i]) {
                    restoreMask |= 1 << i;
                }
            }

            size_t num = std::popcount(restoreMask);
            if (num > gtype.ParityParts()) {
                addLog("not enough parts for restoration/reading");
                errors = true;
            } else {
                if (num) {
                    addLog("some parts missing");
                    errors = true;
                    for (size_t i = 0; i < parts.size() && num < gtype.ParityParts(); ++i) {
                        if (ropes[i]) {
                            ropes[i] = {};
                            restoreMask |= 1 << i;
                            ++num;
                        }
                    }
                    ErasureRestore((TBlobStorageGroupType::ECrcMode)current->CrcMode(), gtype, current->BlobSize(), nullptr,
                        ropes, restoreMask);
                    for (size_t i = 0; i < parts.size(); ++i) {
                        if (parts[i] && parts[i] != ropes[i]) {
                            addLog("part value incorrect");
                            errors = true;
                            break;
                        }
                    }
                } else {
                    unsigned badParts = 0;
                    for (size_t i = 0; i < parts.size(); ++i) {
                        std::vector<TRope> temp(ropes);
                        temp[i] = {};
                        ErasureRestore((TBlobStorageGroupType::ECrcMode)current->CrcMode(), gtype, current->BlobSize(), nullptr,
                            temp, 1 << i);
                        if (temp[i] != ropes[i]) {
                            badParts |= 1 << i;
                        }
                    }
                    if (!badParts) {
                        // all parts are correct
                    } else if (std::popcount(badParts) + 1 == (int)gtype.TotalPartCount()) {
                        int partIdx = std::countr_one(badParts);
                        addLog(TStringBuilder() << "partId# " << partIdx + 1 << " value incorrect");
                        errors = true;
                    } else {
                        addLog("more than one part is incorrect");
                        errors = true;
                    }
                }
            }
        }

        if (log) {
            Cerr << log << Endl;
        }

        std::ranges::fill(parts, TRcBuf());
    };

    auto addItem = [&](TLogoBlobID id, int /*nodeId*/, TFsPath file) {
        if (id.FullID() != current) {
            if (current) {
                finishBucket();
            }
            current.emplace(id.FullID());
        }

        Y_ABORT_UNLESS(id.PartId());
        const ui8 partIdx = id.PartId() - 1;
        Y_ABORT_UNLESS(partIdx < parts.size());

        TRcBuf data(TFileInput(file).ReadAll());
        Y_ABORT_UNLESS(data.size() == gtype.PartSize(id));
        Y_ABORT_UNLESS(!parts[partIdx] || parts[partIdx] == data);
        parts[id.PartId() - 1] = std::move(data);
    };

    for (int i = 2; i < argc; ++i) {
        TString s = argv[i];
        if (!s.EndsWith(".bin")) {
            Cerr << "incorrect filename" << Endl;
            return EXIT_FAILURE;
        }
        s = s.substr(0, s.size() - 4);
        if (size_t pos = s.find('@'); pos != TString::npos) {
            TLogoBlobID id;
            TString explanation;
            int nodeId;
            if (TLogoBlobID::Parse(id, s.substr(0, pos), explanation) && TryFromString(s.substr(pos + 1), nodeId)) {
                addItem(id, nodeId, argv[i]);
            }
        }
    }
    if (current) {
        finishBucket();
    }

    return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}
