select ToPg(Decimal('nan',25,1)), ToPg(Decimal('inf',25,1)), ToPg(Decimal('-inf',25,1)),
  ToPg(Decimal('1.23',25,1)), ToPg(DyNumber("-10.23"));

