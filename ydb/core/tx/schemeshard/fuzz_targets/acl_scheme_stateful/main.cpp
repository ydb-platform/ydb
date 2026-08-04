#include <ydb/core/tx/schemeshard/schemeshard_effective_acl.h>
#include <ydb/library/aclib/aclib.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <array>

using namespace NKikimr::NSchemeShard;

namespace {

constexpr std::array<TStringBuf, 5> Sids = {
    "user0@builtin",
    "user1@builtin",
    "user2@builtin",
    "group0@builtin",
    "svc@staff",
};

struct TNode {
    size_t Parent = 0;
    bool IsContainer = true;
    TString SelfAcl;
    TEffectiveACL EffectiveAcl;
};

ui32 ConsumeRights(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<ui8>(0, 6)) {
        case 0:
            return NACLib::GenericRead;
        case 1:
            return NACLib::GenericUse;
        case 2:
            return NACLib::GenericFull;
        case 3:
            return NACLib::DescribeSchema;
        case 4:
            return NACLib::AlterSchema;
        case 5:
            return NACLib::CreateDirectory | NACLib::CreateTable;
        default:
            return fdp.ConsumeIntegral<ui32>() & NACLib::GenericFull;
    }
}

ui32 ConsumeInheritance(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<ui8>(0, 6)) {
        case 0:
            return NACLib::InheritNone;
        case 1:
            return NACLib::InheritObject;
        case 2:
            return NACLib::InheritContainer;
        case 3:
            return NACLib::InheritOnly;
        case 4:
            return NACLib::InheritObject | NACLib::InheritOnly;
        case 5:
            return NACLib::InheritContainer | NACLib::InheritOnly;
        default:
            return NACLib::DefaultInheritanceType;
    }
}

TString PickSid(FuzzedDataProvider& fdp) {
    return TString(Sids[fdp.ConsumeIntegralInRange<size_t>(0, Sids.size() - 1)]);
}

NACLib::TDiffACL ConsumeDiff(FuzzedDataProvider& fdp) {
    NACLib::TDiffACL diff;
    const ui32 entries = fdp.ConsumeIntegralInRange<ui32>(0, 5);
    for (ui32 i = 0; i < entries; ++i) {
        const auto type = fdp.ConsumeBool() ? NACLib::EAccessType::Allow : NACLib::EAccessType::Deny;
        const ui32 rights = ConsumeRights(fdp);
        const TString sid = PickSid(fdp);
        const ui32 inheritance = ConsumeInheritance(fdp);
        if (fdp.ConsumeBool()) {
            diff.AddAccess(type, rights, sid, inheritance);
        } else {
            diff.RemoveAccess(type, rights, sid, inheritance);
        }
    }

    if (fdp.ConsumeIntegralInRange<ui8>(0, 31) == 0) {
        diff.ClearAccess();
    }
    if (fdp.ConsumeIntegralInRange<ui8>(0, 31) == 0) {
        diff.ClearAccessForSid(PickSid(fdp));
    }

    return diff;
}

NACLib::TACL ParseAcl(const TString& serialized) {
    return serialized ? NACLib::TACL(serialized) : NACLib::TACL();
}

void RecomputeFrom(TVector<TNode>& nodes, size_t index) {
    if (index == 0) {
        nodes[0].EffectiveAcl.Init(nodes[0].SelfAcl);
        index = 1;
    }

    for (size_t i = index; i < nodes.size(); ++i) {
        if (i == 0) {
            continue;
        }
        nodes[i].EffectiveAcl.Update(nodes[nodes[i].Parent].EffectiveAcl, nodes[i].SelfAcl, nodes[i].IsContainer);
    }
}

void CheckAcls(const TVector<TNode>& nodes, FuzzedDataProvider& fdp) {
    for (size_t i = 0; i < nodes.size(); ++i) {
        NACLib::TACL self(nodes[i].EffectiveAcl.GetForSelf());
        const bool childIsContainer = fdp.ConsumeBool();
        NACLib::TACL child(nodes[i].EffectiveAcl.GetForChildren(childIsContainer));
        Y_ABORT_UNLESS(self.SerializeAsString() == nodes[i].EffectiveAcl.GetForSelf());
        Y_ABORT_UNLESS(child.SerializeAsString() == nodes[i].EffectiveAcl.GetForChildren(childIsContainer));

        NACLib::TSecurityObject object("owner@builtin", nodes[i].IsContainer);
        Y_ABORT_UNLESS(object.MutableACL()->ParseFromString(nodes[i].EffectiveAcl.GetForSelf()));
        NACLib::TUserToken token(PickSid(fdp), {TString("group0@builtin")});
        const ui32 rights = ConsumeRights(fdp);
        const bool allowed = object.CheckAccess(rights, token);
        const ui32 effectiveRights = object.GetEffectiveAccessRights(token);
        if (allowed) {
            Y_ABORT_UNLESS((effectiveRights & rights) == rights);
        }
    }
}

void FuzzAcl(FuzzedDataProvider& fdp) {
    TVector<TNode> nodes;
    nodes.reserve(16);

    NACLib::TSecurityObject rootObject("root@builtin", true);
    rootObject.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "root@builtin");
    rootObject.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "group0@builtin");

    TNode root;
    root.SelfAcl = rootObject.GetACL().SerializeAsString();
    root.EffectiveAcl.Init(root.SelfAcl);
    nodes.push_back(root);

    const ui32 steps = fdp.ConsumeIntegralInRange<ui32>(1, 128);
    for (ui32 step = 0; step < steps && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 4)) {
            case 0:
                if (nodes.size() < 16) {
                    TNode node;
                    node.Parent = fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1);
                    node.IsContainer = fdp.ConsumeBool();
                    node.EffectiveAcl.Update(nodes[node.Parent].EffectiveAcl, node.SelfAcl, node.IsContainer);
                    nodes.push_back(node);
                }
                break;
            case 1: {
                const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1);
                NACLib::TACL acl = ParseAcl(nodes[index].SelfAcl);
                acl.ApplyDiff(ConsumeDiff(fdp));
                nodes[index].SelfAcl = acl.SerializeAsString();
                RecomputeFrom(nodes, index);
                break;
            }
            case 2: {
                if (nodes.size() > 1) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(1, nodes.size() - 1);
                    nodes[index].SelfAcl.clear();
                    RecomputeFrom(nodes, index);
                }
                break;
            }
            case 3: {
                if (nodes.size() > 1) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(1, nodes.size() - 1);
                    nodes[index].IsContainer = fdp.ConsumeBool();
                    RecomputeFrom(nodes, index);
                }
                break;
            }
            default:
                CheckAcls(nodes, fdp);
                break;
        }

        if ((step & 7) == 7) {
            CheckAcls(nodes, fdp);
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzAcl(fdp);
    } catch (...) {
    }

    return 0;
}
