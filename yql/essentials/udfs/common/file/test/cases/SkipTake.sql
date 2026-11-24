SELECT ListCollect(ListTake(ListSkip(ParseFile("Int64", "signed.txt"), 1ul), 2ul));
