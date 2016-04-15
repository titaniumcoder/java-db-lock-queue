CREATE TABLE businessobjectlock
(
  uuid VARCHAR(32) NOT NULL,
  id VARCHAR(80) NOT NULL,
  locked_ts TIMESTAMP NOT NULL,
  CONSTRAINT pk_businessobjectlock PRIMARY KEY (uuid),
  CONSTRAINT id_businessobjectlock UNIQUE (id)
);
