DECLARE $quotaName as Utf8?;
DECLARE $browserGroup as Utf8?;
DECLARE $limit as Uint32;
DECLARE $offset as Uint32;
PRAGMA TablePathPrefix ="/Root/";

$browsers = (
    SELECT
    b.id as id,
    q.name AS quota_name,
    b.name AS name,
    b.version AS version,
    b.group AS group,
    b.description AS description,
    bg.browser_platform AS platform,
    MAX_OF(qb.created_at, b.created_at) AS created_at,
    qb.deleted_at AS deleted_at
    FROM
    quotas_browsers_relation AS qb
    LEFT JOIN
    browsers AS b
        ON qb.browser_id = b.id
        LEFT JOIN
        browser_groups AS bg
        ON
        b.group = bg.name
        LEFT JOIN
        quota as q
        ON
        qb.quota_id = q.id
        WHERE
                (
                    ($quotaName IS NOT NULL AND q.name = $quotaName) OR
                    $quotaName IS NULL ) AND        
                    ( ($browserGroup IS NOT NULL AND b.group = $browserGroup) OR  $browserGroup IS NULL
                           ) AND      (            group IS NOT NULL        ));
    
    SELECT * FROM $browsers ORDER BY created_at LIMIT $limit OFFSET $offset;