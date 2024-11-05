/* syntax version 1 */
SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString)) AS DeserializedList
FROM Input;

SELECT Roaring::Serialize(Roaring::Deserialize(binaryString)) AS Serialized
FROM Input;

SELECT ListTake(Roaring::Uint32List(Roaring::Deserialize(binaryString)), 1) AS LimitedList
FROM Input;

SELECT ListTake(ListSkip(Roaring::Uint32List(Roaring::Deserialize(binaryString)), 1), 1) AS OffsetedList
FROM Input;

SELECT ListTake(ListSkip(Roaring::Uint32List(Roaring::Deserialize(binaryString)), 10), 1) AS EmptyList
FROM Input;
