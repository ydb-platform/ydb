select ToBytes(Decimal("14.2",5,1)),
       ToBytes(Decimal("inf",5,1)),
       ToBytes(Decimal("-inf",5,1)),
       ToBytes(Decimal("nan",5,1)),
       ToBytes(Nothing(Optional<Decimal(5,1)>));
       
select FromBytes(ToBytes(Decimal("14.2",5,1)),Decimal(5,1)),
       FromBytes(ToBytes(Decimal("10",5,1)),Decimal(2,1)),
       FromBytes(ToBytes(Decimal("-10",5,1)),Decimal(2,0)),
       FromBytes(ToBytes(Decimal("inf",5,1)),Decimal(5,1)),
       FromBytes(ToBytes(Decimal("-inf",5,1)),Decimal(5,1)),
       FromBytes(ToBytes(Decimal("nan",5,1)),Decimal(5,1)),
       FromBytes(Nothing(String?),Decimal(5,1));

