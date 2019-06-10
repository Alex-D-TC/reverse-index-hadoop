#/bin/sh

tmp_dir=$(mktemp -d)

cd $tmp_dir

hadoop_output_folder=$1
shift

# clear hadoop folder
hdfs dfs -rm -r $hadoop_output_folder
hdfs dfs -mkdir $hadoop_output_folder

for link in "$@"
do
    wget $link
done

java -cp /root/hadoop-reverse-index.jar LinePrepender . augmented

for file in $(ls | grep augmented)
do
    hdfs dfs -copyFromLocal $file $hadoop_output_folder
done

rm -f *

cd
rm -rf tmp_dir
