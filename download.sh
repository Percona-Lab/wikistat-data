for i in {01..12}
do
for d in {01..31}
do
for h in {00..23}
do
        wget http://dumps.wikimedia.org/other/pagecounts-raw/2015/2015-${i}/pagecounts-2015${i}${d}-${h}0000.gz
done
done
done
