DROP TABLE if exists wikistat.wikidata;
CREATE TABLE wikistat.wikidata
(D_Year smallint, D_Month smallint, D_Day smallint, D_hour smallint, project text, pagename text, pageviews integer, bytes bigint)
DISTRIBUTED BY (D_hour);
