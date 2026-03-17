create table if not exists %(table)s (
  id          bigserial    PRIMARY KEY,
  enqueued_at timestamptz  NOT NULL DEFAULT current_timestamp,
  dequeued_at timestamptz,
  expected_at timestamptz,
  schedule_at timestamptz,
  q_name      text         NOT NULL CHECK (length(q_name) > 0),
  data        json         NOT NULL
);

create index if not exists priority_idx_%(name)s on %(table)s
      (schedule_at nulls first, expected_at nulls last, q_name)
    where dequeued_at is null
          and q_name = '%(name)s';

create index if not exists priority_idx_no_%(name)s on %(table)s
    (schedule_at nulls first, expected_at nulls last, q_name)
    where dequeued_at is null
          and q_name != '%(name)s';

drop function if exists pq_notify() cascade;

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(
    case length(new.q_name) > 63
      when true then 'pq_' || md5(new.q_name)
      else new.q_name
    end,
    ''
  );
  return null;
end $$ language plpgsql;

create trigger pq_insert
after insert on %(table)s
for each row
execute procedure pq_notify();
