/* syntax version 1 */
SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString)) AS DeserializedList
FROM Input;

SELECT Roaring::Serialize(Roaring::Deserialize(binaryString)) AS Serialized
FROM Input;

SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString), <|listLimit: 1, listOffset:0|>) AS LimitedList
FROM Input;

SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString), <|listLimit: 1, listOffset:1|>) AS OffsetedList
FROM Input;

SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString), <|listLimit: 1, listOffset:10|>) AS OffsetedList
FROM Input;
