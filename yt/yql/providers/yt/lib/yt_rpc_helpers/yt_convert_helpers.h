#include <yt/cpp/mapreduce/interface/common.h>

namespace NYql::NDqs {
    
template<typename T>
void ConfigureTransaction(T& request, const ui32* dw) {
    // P. S. No proper way to convert it
    request->mutable_transactional_options()->mutable_transaction_id()->set_first((ui64)dw[3] | (ui64(dw[2]) << 32));
    request->mutable_transactional_options()->mutable_transaction_id()->set_second((ui64)dw[1] | (ui64(dw[0]) << 32));
}
template<typename T>
void ConfigureTransaction(T& request, NYT::TRichYPath& richYPath) {
    
    if (richYPath.TransactionId_) {
        ConfigureTransaction(request, richYPath.TransactionId_->dw);
    }
}
};