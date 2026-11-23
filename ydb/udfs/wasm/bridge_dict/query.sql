/* syntax version 1 */
SELECT Unwrap(BridgeDict::Lookup(AsDict(AsTuple("a", 42l)), "a")) AS hit;
