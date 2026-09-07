#include <ydb/core/tx/schemeshard/schemeshard_audit_log_fragment.h>
#include <ydb/core/tx/schemeshard/schemeshard_path_footprint.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <google/protobuf/descriptor.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

NKikimrSchemeOp::TModifyScheme MakeTx(NKikimrSchemeOp::EOperationType type, const TString& workingDir) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetOperationType(type);
    tx.SetWorkingDir(workingDir);
    return tx;
}

}  // namespace

Y_UNIT_TEST_SUITE(TSchemeShardPathFootprintExtract) {
    Y_UNIT_TEST(RenderRepeatedAndMapFields) {
        TPathRef ref;
        ref.Field = EPathField::CopyTables_Item_IndexImplDropCdc_StreamName;
        ref.Index = 12;
        ref.SubIndex = 345;
        ref.MapKey = "key{i}";
        UNIT_ASSERT_VALUES_EQUAL(FieldPath(ref),
            "CreateConsistentCopyTables.CopyTableDescriptions[12]"
            ".IndexImplTableDropCdcStreams[key{i}].StreamName[345]");

        ref.Index = 0;
        ref.SubIndex = Max<ui32>();
        ref.MapKey = "";
        UNIT_ASSERT_VALUES_EQUAL(FieldPath(ref),
            "CreateConsistentCopyTables.CopyTableDescriptions[0]"
            ".IndexImplTableDropCdcStreams[].StreamName[4294967295]");

        ref.Field = EPathField::MkDir_Name;
        UNIT_ASSERT_VALUES_EQUAL(FieldPath(ref), "MkDir.Name");
    }

    Y_UNIT_TEST(EveryPathFieldRendersAndIsListedOnce) {
        const size_t count = static_cast<size_t>(EPathField::Count);
        UNIT_ASSERT_C(count > 100, "the field table has only " << count << " rows");

        THashSet<TString> templates;
        THashSet<TString> protoNames;
        size_t synthetic = 0;
        for (size_t i = 0; i < count; ++i) {
            const auto field = static_cast<EPathField>(i);
            const TString tmpl(PathFieldName(field));
            UNIT_ASSERT_C(!tmpl.empty(), "field " << i << " has no field-path template");
            UNIT_ASSERT_C(templates.insert(tmpl).second,
                "two path fields share the field-path template " << tmpl);

            TPathRef ref;
            ref.Field = field;
            ref.Index = 3;
            ref.SubIndex = 7;
            ref.MapKey = "someKey";
            const TString rendered = FieldPath(ref);
            UNIT_ASSERT_C(rendered.find('{') == TString::npos
                    && rendered.find('}') == TString::npos,
                "unexpanded placeholder in " << rendered);
            if (tmpl.Contains("{i}")) {
                UNIT_ASSERT_C(rendered.Contains("[3]"), rendered);
            }
            if (tmpl.Contains("{j}")) {
                UNIT_ASSERT_C(rendered.Contains("[7]"), rendered);
            }
            if (tmpl.Contains("{key}")) {
                UNIT_ASSERT_C(rendered.Contains("[someKey]"), rendered);
            }
            if (tmpl.find('{') == TString::npos) {
                UNIT_ASSERT_VALUES_EQUAL(rendered, tmpl);
            }

            const TString proto(PathFieldProtoName(field));
            if (proto.empty()) {
                ++synthetic;
            } else {
                protoNames.insert(proto);
            }
        }
        UNIT_ASSERT_C(synthetic > 0, "no synthetic (marker or id) field rows");

        const auto& known = KnownPathFieldNames();
        THashSet<TString> knownSet;
        for (const TStringBuf name : known) {
            UNIT_ASSERT_C(!name.empty(), "KnownPathFieldNames() has an empty entry");
            UNIT_ASSERT_C(knownSet.insert(TString(name)).second,
                "KnownPathFieldNames() lists " << name << " twice");
        }
        UNIT_ASSERT_VALUES_EQUAL(known.size(), protoNames.size());
        for (const auto& name : protoNames) {
            UNIT_ASSERT_C(knownSet.contains(name),
                name << " is in the field table but not in KnownPathFieldNames()");
        }
        UNIT_ASSERT_C(IsSorted(known.begin(), known.end()),
            "KnownPathFieldNames() is not sorted");
    }

    // Extraction reads the request, it does not copy it: every value is a view
    // into the TModifyScheme that was passed in. Only the resolve step, which
    // has to outlive the request, materializes strings.
    Y_UNIT_TEST(ExtractedValuesPointIntoTheRequest) {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpMoveTable, "/MyRoot");
        tx.MutableMoveTable()->SetSrcPath("/MyRoot/Src");
        tx.MutableMoveTable()->SetDstPath("/MyRoot/Dst");

        const auto refs = ExtractPathRefs(tx);
        UNIT_ASSERT_VALUES_EQUAL(refs.size(), 3u);
        UNIT_ASSERT_EQUAL(refs[0].Value.data(), tx.GetMoveTable().GetSrcPath().data());
        UNIT_ASSERT_EQUAL(refs[1].Value.data(), tx.GetMoveTable().GetDstPath().data());

        // A sibling base is a view too, when the request spells it out.
        auto move = MakeTx(NKikimrSchemeOp::ESchemeOpMoveIndex, "/MyRoot");
        move.MutableMoveIndex()->SetTablePath("/MyRoot/Table");
        move.MutableMoveIndex()->SetSrcPath("oldIndex");
        const auto moveRefs = ExtractPathRefs(move);
        UNIT_ASSERT_EQUAL(moveRefs[1].Value.data(), move.GetMoveIndex().GetSrcPath().data());
        UNIT_ASSERT_EQUAL(moveRefs[1].BasePath.data(), move.GetMoveIndex().GetTablePath().data());
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardAuditLogPaths) {

TString AuditPaths(const NKikimrSchemeOp::TModifyScheme& tx) {
    return JoinSeq(",", MakeAuditLogFragment(tx).Paths);
}

// Everything below reproduces what the hand-written switch produced, byte for
// byte. A change here is a change to what audit consumers read.
Y_UNIT_TEST(UnchangedFamiliesKeepTheirPaths) {
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpMkDir, "/MyRoot");
        tx.MutableMkDir()->SetName("dir");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/dir");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateTable, "/MyRoot");
        tx.MutableCreateTable()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropTable, "/MyRoot");
        tx.MutableDrop()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterTable, "/MyRoot");
        tx.MutableAlterTable()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpModifyACL, "/MyRoot");
        tx.MutableModifyACL()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateSubDomain, "/MyRoot");
        tx.MutableSubDomain()->SetName("db");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/db");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterUserAttributes, "/MyRoot");
        tx.MutableAlterUserAttributes()->SetPathName("sub");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/sub");
    }
    {
        // Source and target, in that order, both absolute.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpMoveTable, "/MyRoot");
        tx.MutableMoveTable()->SetSrcPath("/MyRoot/a");
        tx.MutableMoveTable()->SetDstPath("/MyRoot/b");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/a,/MyRoot/b");
    }
    {
        // Two leaves under an absolute base.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpMoveIndex, "/MyRoot");
        tx.MutableMoveIndex()->SetTablePath("/MyRoot/T");
        tx.MutableMoveIndex()->SetSrcPath("i1");
        tx.MutableMoveIndex()->SetDstPath("i2");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/i1,/MyRoot/T/i2");
    }
    {
        // The table is the parent of what changes, so only the index shows up.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropIndex, "/MyRoot");
        tx.MutableDropIndex()->SetTableName("T");
        tx.MutableDropIndex()->SetIndexName("i");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/i");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateCdcStream, "/MyRoot");
        tx.MutableCreateCdcStream()->SetTableName("T");
        tx.MutableCreateCdcStream()->MutableStreamDescription()->SetName("S");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/S");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropCdcStream, "/MyRoot");
        tx.MutableDropCdcStream()->SetTableName("T");
        tx.MutableDropCdcStream()->AddStreamName("S1");
        tx.MutableDropCdcStream()->AddStreamName("S2");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/S1,/MyRoot/T/S2");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpRotateCdcStream, "/MyRoot");
        tx.MutableRotateCdcStream()->SetTableName("T");
        tx.MutableRotateCdcStream()->SetOldStreamName("O");
        tx.MutableRotateCdcStream()->MutableNewStream()
            ->MutableStreamDescription()->SetName("N");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/O,/MyRoot/T/N");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateIndexBuild, "/MyRoot");
        tx.MutableInitiateIndexBuild()->SetTable("/MyRoot/T");
        tx.MutableInitiateIndexBuild()->MutableIndex()->SetName("i");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T/i");
    }
    {
        // The index and its impl table are dependencies of the create, not
        // paths the request names as changing.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateIndexedTable, "/MyRoot");
        tx.MutableCreateIndexedTable()->MutableTableDescription()->SetName("T");
        tx.MutableCreateIndexedTable()->AddIndexDescription()->SetName("i");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        // No path field at all: the working dir is the audience being altered.
        // The login sub-message is set because the record's operation name is
        // derived from it, not because the paths depend on it.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterLogin, "/MyRoot");
        tx.MutableAlterLogin()->MutableCreateGroup()->SetGroup("g");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot");
    }
    {
        // Same shape for a different reason: the aggregator's working dir
        // already points at the backup collection.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateFullBackupOp,
            "/MyRoot/.backups/collections/c");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/.backups/collections/c");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets, "/MyRoot");
        tx.MutableIncrementalRestoreLockTargets()->AddDstPaths("d");
        tx.MutableIncrementalRestoreLockTargets()->AddSrcPaths("s");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/d,/MyRoot/s");
    }
}

Y_UNIT_TEST(IdAddressedRequestsKeepLegacyPaths) {
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropTable, "/MyRoot");
        tx.MutableDrop()->SetId(36);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableDrop()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");

        const auto refs = ExtractPathRefs(tx);
        UNIT_ASSERT(refs[0].Kind == EPathRefKind::ById);
        UNIT_ASSERT_VALUES_EQUAL(refs[0].LocalPathId, 36u);
        UNIT_ASSERT(JoinPathRef(tx.GetWorkingDir(), refs[0], {}).empty());
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterTable, "/MyRoot");
        TPathId(TOwnerId(72057594046678944ull), TLocalPathId(7)).ToProto(
            tx.MutableAlterTable()->MutablePathId());
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableAlterTable()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterTable, "/MyRoot");
        tx.MutableAlterTable()->SetId_Deprecated(7);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableAlterTable()->SetName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup, "/MyRoot");
        tx.MutableAlterPersQueueGroup()->SetPathId(7);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableAlterPersQueueGroup()->SetName("topic");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/topic");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume, "/MyRoot");
        tx.MutableAlterBlockStoreVolume()->SetPathId(7);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableAlterBlockStoreVolume()->SetName("volume");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/volume");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions, "/MyRoot");
        tx.MutableSplitMergeTablePartitions()->SetTableOwnerId(72057594046678944ull);
        tx.MutableSplitMergeTablePartitions()->SetTableLocalId(7);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/");
        tx.MutableSplitMergeTablePartitions()->SetTablePath("/MyRoot/T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot//MyRoot/T");
    }
    for (const auto type : {NKikimrSchemeOp::ESchemeOpAlterReplication,
            NKikimrSchemeOp::ESchemeOpAlterTransfer}) {
        auto tx = MakeTx(type, "/MyRoot");
        TPathId(TOwnerId(72057594046678944ull), TLocalPathId(7)).ToProto(
            tx.MutableAlterReplication()->MutablePathId());
        UNIT_ASSERT(MakeAuditLogFragment(tx).Paths.empty());
        tx.MutableAlterReplication()->SetName("repl");
        UNIT_ASSERT(MakeAuditLogFragment(tx).Paths.empty());
    }
    for (const auto type : {NKikimrSchemeOp::ESchemeOpDropResourcePool,
            NKikimrSchemeOp::ESchemeOpDropStreamingQuery}) {
        auto tx = MakeTx(type, "/MyRoot");
        tx.MutableDrop()->SetId(7);
        const auto paths = MakeAuditLogFragment(tx).Paths;
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
        UNIT_ASSERT(paths[0].empty());
        tx.MutableDrop()->SetName("name");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "name");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpMkDir, "/MyRoot");
        tx.MutableMkDir()->SetName("dir");
        tx.AddApplyIf()->SetPathId(7);
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/dir");
    }
}

// These families logged a leaf name with no directory in front of it, because
// their switch arm forgot the working dir.
Y_UNIT_TEST(LeafNamesAreJoinedToTheWorkingDir) {
    const TString pools = "/MyRoot/.metadata/workload_manager/pools";
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateResourcePool, pools);
        tx.MutableCreateResourcePool()->SetName("MyResourcePool");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), pools + "/MyResourcePool");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterResourcePool, pools);
        tx.MutableCreateResourcePool()->SetName("MyResourcePool");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), pools + "/MyResourcePool");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropResourcePool, pools);
        tx.MutableDrop()->SetName("MyResourcePool");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), pools + "/MyResourcePool");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateStreamingQuery, "/MyRoot");
        tx.MutableCreateStreamingQuery()->SetName("Q");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/Q");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery, "/MyRoot");
        tx.MutableCreateStreamingQuery()->SetName("Q");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/Q");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropStreamingQuery, "/MyRoot");
        tx.MutableDrop()->SetName("Q");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/Q");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpTruncateTable, "/MyRoot");
        tx.MutableTruncateTable()->SetTableName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
}

// SplitMerge resolves TablePath absolutely (split_merge.cpp:849); joining the
// working dir in front of it produced "/MyRoot//MyRoot/T".
Y_UNIT_TEST(AnAbsolutePathIsNotJoinedToTheWorkingDir) {
    auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions, "/MyRoot");
    tx.MutableSplitMergeTablePartitions()->SetTablePath("/MyRoot/T");
    UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
}

// Four arms used to fall through with an empty body, so the record carried no
// "paths" field for an operation that changes exactly one path.
Y_UNIT_TEST(FamiliesThatUsedToReportNothing) {
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterSequence, "/MyRoot");
        tx.MutableSequence()->SetName("seq");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/seq");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterReplication, "/MyRoot");
        tx.MutableAlterReplication()->SetName("repl");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/repl");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterTransfer, "/MyRoot");
        tx.MutableAlterReplication()->SetName("transfer");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/transfer");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterExternalTable, "/MyRoot");
        tx.MutableCreateExternalTable()->SetName("et");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/et");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterExternalDataSource, "/MyRoot");
        tx.MutableCreateExternalDataSource()->SetName("ds");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/ds");
    }
}

// The create arm read AlterColumnTable.Name, a field a create request does not
// fill, so the record showed the bare working dir.
Y_UNIT_TEST(CreateColumnTableNamesWhatItCreates) {
    auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateColumnTable, "/MyRoot");
    tx.MutableCreateColumnTable()->SetName("ct");
    UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/ct");
}

// New entries rather than corrected ones: a copy reads its source, and the
// extractor records that as a Source ref. The old switch dropped it.
Y_UNIT_TEST(CopySourcesAreReported) {
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateTable, "/MyRoot");
        tx.MutableCreateTable()->SetName("dst");
        tx.MutableCreateTable()->SetCopyFromTable("/MyRoot/src");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/dst,/MyRoot/src");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables, "/MyRoot");
        auto* item = tx.MutableCreateConsistentCopyTables()->AddCopyTableDescriptions();
        item->SetSrcPath("/MyRoot/src");
        item->SetDstPath("/MyRoot/dst");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/src,/MyRoot/dst");
    }
}

// Also new entries: the continuous-backup family creates a cdc stream and an
// incremental backup table beside the table it names, and the request spells
// both out when the client chose their names.
Y_UNIT_TEST(ContinuousBackupReportsWhatItCreates) {
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateContinuousBackup, "/MyRoot");
        tx.MutableCreateContinuousBackup()->SetTableName("T");
        tx.MutableCreateContinuousBackup()->MutableContinuousBackupDescription()
            ->SetStreamName("S");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T,/MyRoot/T/S");
    }
    {
        // Without a stream name the schemeshard generates one from the clock,
        // so there is nothing to report and the output is what it always was.
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpCreateContinuousBackup, "/MyRoot");
        tx.MutableCreateContinuousBackup()->SetTableName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpAlterContinuousBackup, "/MyRoot");
        tx.MutableAlterContinuousBackup()->SetTableName("T");
        auto& take = *tx.MutableAlterContinuousBackup()->MutableTakeIncrementalBackup();
        take.SetDstPath("bak");
        take.SetDstStreamPath("S");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T,/MyRoot/bak,/MyRoot/T/S");
    }
    {
        auto tx = MakeTx(NKikimrSchemeOp::ESchemeOpDropContinuousBackup, "/MyRoot");
        tx.MutableDropContinuousBackup()->SetTableName("T");
        UNIT_ASSERT_VALUES_EQUAL(AuditPaths(tx), "/MyRoot/T");
    }
}
}
