; ModuleID = '128_bit_ir.cpp'
source_filename = "128_bit_ir.cpp"
target datalayout = "e-m:e-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-unknown-linux-gnu"

; Function Attrs: alwaysinline norecurse nounwind uwtable
define void @sum_sqr_128_ir(i128* nocapture, i128* nocapture readonly, i128* nocapture readonly) local_unnamed_addr #0 {
  %4 = load i128, i128* %1, align 16, !tbaa !2
  %5 = mul nsw i128 %4, %4
  %6 = load i128, i128* %2, align 16, !tbaa !2
  %7 = mul nsw i128 %6, %6
  %8 = add nuw nsw i128 %7, %5
  store i128 %8, i128* %0, align 16, !tbaa !2
  ret void
}

attributes #0 = { alwaysinline norecurse nounwind uwtable "correctly-rounded-divide-sqrt-fp-math"="false" "disable-tail-calls"="false" "less-precise-fpmad"="false" "no-frame-pointer-elim"="false" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="false" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+fxsr,+mmx,+sse,+sse2,+x87" "unsafe-fp-math"="false" "use-soft-float"="false" }

!llvm.module.flags = !{!0}
!llvm.ident = !{!1}

!0 = !{i32 1, !"wchar_size", i32 4}
!1 = !{!"clang version 5.0.0 (tags/RELEASE_500/final 316565)"}
!2 = !{!3, !3, i64 0}
!3 = !{!"__int128", !4, i64 0}
!4 = !{!"omnipotent char", !5, i64 0}
!5 = !{!"Simple C++ TBAA"}
