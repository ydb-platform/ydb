/* syntax version 1 */
-- One row. Use with YDB_WASM_STRING_DEBUG=1 on the tenant node.
SELECT id,
       Text::count_letters(txt) AS letters,
       Text::byte_at(txt, 0) AS b0,
       Text::text_length(txt) AS len
FROM `text_200kb`
WHERE id = 1ul;
