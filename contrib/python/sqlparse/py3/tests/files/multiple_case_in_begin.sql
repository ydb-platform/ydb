CREATE TRIGGER mytrig
AFTER UPDATE OF vvv ON mytable
BEGIN
    UPDATE aa
        SET mycola = (CASE WHEN (A=1) THEN 2 END);
    UPDATE bb
        SET mycolb = (CASE WHEN (B=1) THEN 5 END);
END;