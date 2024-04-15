/* syntax version 1 */
SELECT Roaring::Uint32List(Roaring::Deserialize(binaryString)) AS DeserializedList
FROM Input;

SELECT Roaring::Serialize(Roaring::Deserialize(binaryString)) AS Serialized
FROM Input;
