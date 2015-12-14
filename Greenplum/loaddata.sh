for i in {01..04}
do
for d in {01..31}
do
for h in {00..23}
do
psql -c "DROP EXTERNAL TABLE wikistat.ext_load_wiki_2015${i}${d}${h};" tutorial
psql -c "CREATE EXTERNAL TABLE wikistat.ext_load_wiki_2015${i}${d}${h} (project text, pagename text, pageviews integer, bytes bigint) LOCATION ('gpfdist://localhost:8081/pagecounts-2015${i}${d}-${h}0000.gz') FORMAT 'TEXT' ( DELIMITER ' ' ESCAPE 'OFF' ) LOG ERRORS INTO wikistat.wiki_load_errors SEGMENT REJECT LIMIT 50000 rows;" tutorial
psql -c "INSERT into wikidata SELECT 2015,${i},${d},${h},project,pagename,pageviews,bytes FROM ext_load_wiki_2015${i}${d}${h};" tutorial
done
done
done
