#include "flat_part_iter.h"

void NKikimr::NTable::TPartIter::Apply(TRowState& row, TPinout::TPin pin, const NPage::TDataPage::TRecord* data,
                const TPartScheme::TColumn& info) const noexcept
{
    auto op = data->GetCellOp(info);

    if (op == ECellOp::Empty) {
        Y_ABORT_UNLESS(!info.IsKey(), "Got an absent key cell");
    } else if (op == ELargeObj::Inline) {
        row.Set(pin.To, op, data->Cell(info));
    } else if (op == ELargeObj::Extern || op == ELargeObj::Outer) {
        const auto ref = data->Cell(info).AsValue<ui64>();

        row.AddExternalBlobSize(Env->PageSize(Part, ref, op));

        if (ref >> (sizeof(ui32) * 8))
            Y_ABORT("Upper bits of ELargeObj ref now isn't used");
        if (auto blob = Env->Locate(Part, ref, op)) {
            const auto got = NPage::TLabelWrapper().Read(**blob);

            Y_ABORT_UNLESS(got == NPage::ECodec::Plain && got.Version == 0);

            row.Set(pin.To, { ECellOp(op), ELargeObj::Inline }, TCell(*got));
        } else if (op == ELargeObj::Outer) {
            op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::Outer);

            row.Set(pin.To, op, { } /* cannot put some useful data */);
        } else {
            Y_ABORT_UNLESS(ref < (*Part->Blobs)->size(), "out of blobs catalog");

            /* Have to preserve reference to memory with TGlobId until
                of next iterator alteration method invocation. This is
                why here direct array of TGlobId is used.
            */
            if (IgnoreMissingExternalBlobs) {
                row.IncMissingExternalBlobs();
                op = TCellOp(ECellOp(op), ELargeObj::GlobId);
            } else {
                op = TCellOp(blob.Need ? ECellOp::Null : ECellOp(op), ELargeObj::GlobId);
            }

            row.Set(pin.To, op, TCell::Make((**Part->Blobs)[ref]));
        }
    } else {
        Y_ABORT("Got an unknown blob placement reference type");
    }
}
