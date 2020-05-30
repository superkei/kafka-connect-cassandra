drop table example;

create table example (
  "id" text,
  "value" text,
  "ts" timestamp,
  PRIMARY KEY (("id"), "ts")
)
WITH CLUSTERING ORDER BY ("ts" DESC);
