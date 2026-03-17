CREATE TRIGGER IF NOT EXISTS remove_if_it_was_the_last_file_link
-- Delete the direntry when is removed it's last static link
    AFTER DELETE ON links
    WHEN NOT EXISTS
    (
        SELECT * FROM links
        WHERE child_entry = OLD.child_entry
        LIMIT 1
    )
BEGIN
    DELETE FROM dir_entries
    WHERE dir_entries.inode = OLD.child_entry;
END;