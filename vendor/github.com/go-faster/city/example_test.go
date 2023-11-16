package city_test

import (
	"fmt"

	"github.com/go-faster/city"
)

func ExampleHash32() {
	s := []byte("hello")
	hash32 := city.Hash32(s)
	fmt.Printf("the 32-bit hash of 'hello' is: %#x\n", hash32)

	// Output:
	// the 32-bit hash of 'hello' is: 0x79969366
}

func ExampleHash64() {
	s := []byte("hello")
	hash64 := city.Hash64(s)
	fmt.Printf("the 64-bit hash of 'hello' is: %#x\n", hash64)

	// Output:
	// the 64-bit hash of 'hello' is: 0xb48be5a931380ce8
}

func ExampleHash128() {
	fmt.Println(city.Hash128([]byte("hello")))

	// Output: {8030732511675000650 7283604105673962311}
}

func ExampleCH64() {
	// See https://github.com/ClickHouse/ClickHouse/issues/8354
	/*
		SELECT cityHash64('Moscow')
		┌─cityHash64('Moscow')─┐
		│ 12507901496292878638 │
		└──────────────────────┘
		SELECT farmHash64('Moscow')
		┌─farmHash64('Moscow')─┐
		│  5992710078453357409 │
		└──────────────────────┘
	*/
	s := []byte("Moscow")
	fmt.Print("ClickHouse: ")
	fmt.Println(city.CH64(s))
	fmt.Print("CityHash:   ")
	fmt.Println(city.Hash64(s))
	// Output:
	// ClickHouse: 12507901496292878638
	// CityHash:   5992710078453357409
}
