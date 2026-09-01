#include <ydb/core/tx/schemeshard/schemeshard_affected_paths.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

// DeclareChildOfWorkingDir is a free function over strings: no tablet, no runtime, no
// test env. Keep it that way -- the point of these cases is that the declaration shape
// is checkable without standing up a schemeshard.

Y_UNIT_TEST_SUITE(TAffectedPathsTest) {

Y_UNIT_TEST(ChildOfWorkingDirIsACreate) {
    const TAffectedPaths declared = DeclareChildOfWorkingDir("/MyRoot/DirA", "DirB");

    UNIT_ASSERT_VALUES_EQUAL(declared.Paths.size(), 2u);

    const TAffectedPath& target = declared.Paths[0];
    UNIT_ASSERT_VALUES_EQUAL(target.Path, "/MyRoot/DirA/DirB");
    UNIT_ASSERT(target.Role == TAffectedPath::ERole::Target);
    UNIT_ASSERT(target.Effect == TAffectedPath::EEffect::Create);
    UNIT_ASSERT(target.Class == TAffectedPath::EEffectClass::SchemaEffect);
    UNIT_ASSERT(target.Expect == TAffectedPath::EObservation::MustWrite);

    const TAffectedPath& container = declared.Paths[1];
    UNIT_ASSERT_VALUES_EQUAL(container.Path, "/MyRoot/DirA");
    UNIT_ASSERT(container.Role == TAffectedPath::ERole::Container);
    UNIT_ASSERT(container.Effect == TAffectedPath::EEffect::ChildrenChanged);
    // MayWrite, and this assertion used to say MustWrite. Gaining a child usually does bump
    // the parent's DirAlterVersion -- but not always: creating a vector index's impl table
    // under /MyRoot/Table/index1 leaves that index's own row untouched, which the reverse
    // check caught in TVectorIndexTests::ReplaceVectorIndex. A container cannot promise a
    // write on behalf of an operation it knows nothing about.
    UNIT_ASSERT(container.Expect == TAffectedPath::EObservation::MayWrite);
}

Y_UNIT_TEST(ChildOfWorkingDirCanonizes) {
    // WorkingDir arrives from the wire and may carry a trailing slash. A plain join would
    // produce "/MyRoot/DirA//DirB", a path that matches nothing but would still be
    // recorded verbatim.
    const TAffectedPaths declared = DeclareChildOfWorkingDir("/MyRoot/DirA/", "DirB");

    UNIT_ASSERT_VALUES_EQUAL(declared.Paths.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(declared.Paths[0].Path, "/MyRoot/DirA/DirB");
}

Y_UNIT_TEST(ChildOfWorkingDirRelativeName) {
    // A create may name a relative path rather than a leaf. The directory that gains the
    // child is then the target's parent, NOT WorkingDir. Load-bearing: declaring
    // WorkingDir here would name the right container only for a bare leaf.
    const TAffectedPaths declared = DeclareChildOfWorkingDir("/MyRoot/DirA", "DirB/DirC");

    UNIT_ASSERT_VALUES_EQUAL(declared.Paths.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(declared.Paths[0].Path, "/MyRoot/DirA/DirB/DirC");
    UNIT_ASSERT_VALUES_EQUAL(declared.Paths[1].Path, "/MyRoot/DirA/DirB");
    UNIT_ASSERT(declared.Paths[1].Role == TAffectedPath::ERole::Container);
}

Y_UNIT_TEST(RootChildHasNoContainer) {
    // A target directly under the root has no container path to name. Pushing one anyway
    // would put an empty string in the outbox record.
    const TAffectedPaths declared = DeclareChildOfWorkingDir("/", "MyRoot");

    UNIT_ASSERT_VALUES_EQUAL(declared.Paths.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(declared.Paths[0].Path, "/MyRoot");
    UNIT_ASSERT(declared.Paths[0].Role == TAffectedPath::ERole::Target);
}

Y_UNIT_TEST(DefaultsPreserveExistingMeaning) {
    // The typed effect model is additive: the 125 per-op declarations set none of these
    // fields, so the defaults must carry what those declarations already meant.
    const TAffectedPath defaulted;

    UNIT_ASSERT(defaulted.Class == TAffectedPath::EEffectClass::SchemaEffect);
    UNIT_ASSERT(defaulted.Effect == TAffectedPath::EEffect::Alter);
    UNIT_ASSERT(defaulted.Expect == TAffectedPath::EObservation::MayWrite);
}

// The reverse half of the cross-check. ObservePathTouched answers "did the operation write
// a path it never declared"; these cases answer "did the operation declare a path it never
// wrote", which is the direction an over-declaration hides in.

Y_UNIT_TEST(UnfulfilledMustWriteIsReported) {
    TAffectedPaths declared;
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirA",
        .Expect = TAffectedPath::EObservation::MustWrite,
    });
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirA/DirB",
        .Expect = TAffectedPath::EObservation::MustWrite,
    });

    const THashSet<TString> observed = {"/MyRoot/DirA"};

    const auto missed = FindUnfulfilledMustWrite({declared}, observed);
    UNIT_ASSERT(missed.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*missed, "/MyRoot/DirA/DirB");
}

Y_UNIT_TEST(FulfilledMustWriteIsSilent) {
    TAffectedPaths declared;
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirA",
        .Expect = TAffectedPath::EObservation::MustWrite,
    });

    const THashSet<TString> observed = {"/MyRoot/DirA"};

    UNIT_ASSERT(!FindUnfulfilledMustWrite({declared}, observed).has_value());
}

Y_UNIT_TEST(MayWriteAndReferenceOnlyNeedNoWrite) {
    // The whole point of the three-valued intent: only MustWrite is a claim strong enough
    // to fail on. A successful no-op is legitimate for MayWrite, and required of
    // ReferenceOnly -- demanding a write of either would make the check unusable.
    TAffectedPaths declared;
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirA",
        .Expect = TAffectedPath::EObservation::MayWrite,
    });
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirB",
        .Expect = TAffectedPath::EObservation::ReferenceOnly,
    });

    UNIT_ASSERT(!FindUnfulfilledMustWrite({declared}, THashSet<TString>{}).has_value());
}

Y_UNIT_TEST(ExemptDeclarationDemandsNothing) {
    // nullopt means the operation type is exempt, not that it affects nothing. Reading it
    // as an empty declaration would be harmless here, but reading an *empty* declaration as
    // exempt would not -- keep the two distinguishable.
    TVector<std::optional<TAffectedPaths>> declared;
    declared.push_back(std::nullopt);

    UNIT_ASSERT(!FindUnfulfilledMustWrite(declared, THashSet<TString>{}).has_value());
}

Y_UNIT_TEST(IncompleteDeclarationDemandsNothing) {
    // A cascade drop knows its entry points but not the subtree it walks. Its own entries
    // may still be MustWrite, but the operation was never in a position to promise the
    // list was whole, so the reverse check cannot hold it to one.
    TAffectedPaths declared;
    declared.Incomplete = true;
    declared.Paths.push_back(TAffectedPath{
        .Path = "/MyRoot/DirA",
        .Expect = TAffectedPath::EObservation::MustWrite,
    });

    UNIT_ASSERT(!FindUnfulfilledMustWrite({declared}, THashSet<TString>{}).has_value());
}

}
