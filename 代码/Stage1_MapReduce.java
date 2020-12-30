package nju;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.io.nio.SelectorManager;

class Item implements Serializable,Comparable<Item>{
    public String id;
    public long num;
    public Item(String id, long num){
        this.id=id;
        this.num=num;
    }
    @Override
    public int compareTo(Item o){
        return -Long.compare(num,o.num);
    }
}

class User implements Serializable,Comparable<User>{
    public String user_id,age_range;
    public User(String user_id, String age_range){
        this.user_id=user_id;
        this.age_range=age_range;
    }
    @Override
    public int compareTo(User o){return o.user_id.compareTo(user_id);}
}

public class TaobaoPredict_Stage1{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text id = new Text();

        private static Set<User> users= new TreeSet<User>();
        private String localFiles;
        private int task;
        
        //建立用户信息表
		@Override
		public void setup(Context context) throws IOException,InterruptedException{
			Configuration conf = context.getConfiguration();
			localFiles  = conf.getStrings("user_info")[0];
            System.out.println(localFiles);
            task=conf.getInt("task_type",1);
			FileSystem fs = FileSystem.get(URI.create(localFiles), conf);  
			FSDataInputStream hdfsInStream = fs.open(new Path(localFiles));  
			//从hdfs中读取user_info
			InputStreamReader isr = new InputStreamReader(hdfsInStream, "utf-8");  
			String line;
			BufferedReader br = new BufferedReader(isr);
			while ((line = br.readLine()) != null) {
				String[] str=line.split(",");
				String user_id,age_range;
                user_id=str[0];
				if(str.length==1 || (str.length==2 && !line.substring(line.length() - 1).equals(",")))
                    age_range="-1";
				else
				    age_range=str[1];

                // <18岁为1;[18,24]为2;[25,29]为3;[30,34]为4;[35,39]为5;[40,49]为6;>=50时为7和8;0和NULL表示未知
                // 为了减少运行时间，仅记录年轻人的user_info
                if(age_range.equals("1") || age_range.equals("2") || age_range.equals("3"))
				{
                    User u = new User(user_id, age_range);
                    users.add(u);
                }
			}
            System.out.println("Setup Succeed!");
		}

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] str=line.split(",");
            String user_id=str[0];
            String item_id=str[1];
            String cat_id=str[2];
            String merchant_id=str[3];
            String brand_id,time_stamp,action_type;
            if(str.length==7){
                brand_id=str[4];
                time_stamp=str[5];
                action_type=str[6];
            }
            else{
                time_stamp=str[4];
                action_type=str[5];
            }
            if(!time_stamp.equals("1111"))
                return;
            switch (task){
                case 1: //双⼗⼀最热⻔商品
                    if(!action_type.equals("0")){
                        id.set(item_id);
                        System.out.println("item id:"+id+" recorded!");
                        context.write(id, one);
                        return;
                    }
                    break;
                case 2: //最受年轻⼈(age<30)关注的商家
                    if(!action_type.equals("0")){
                        for(User u:users){
                            if(u.user_id.equals(user_id)){
                                id.set(merchant_id);
                                context.write(id, one);
                                return;
                            }
                        }
                    }
                    break;
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private TreeSet<Item> tree = new TreeSet<Item>();
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Item item= new Item(key.toString(), (long)sum);
            if (tree.size()<100||tree.last().num<item.num){
                tree.add(item);
            }
            if (tree.size()>100){
                tree.pollLast();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            while(!tree.isEmpty()){
                result.set((int)tree.first().num);
                context.write(new Text(tree.first().id), result);
                tree.pollFirst();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int task=Integer.parseInt(args[1]);
        Configuration conf1 = new Configuration();
		conf1.setStrings("user_info","hdfs://localhost:9000/input1/user_info_format1.csv");
		conf1.setInt("task_type",task);
		Job job1 = Job.getInstance(conf1);
		if(task==1)
		    job1.setJobName("most popular item");
		else
            job1.setJobName("most popular merchant");
        job1.setJarByClass(TaobaoPredict_Stage1.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/input2"));
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/output"));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        System.out.println("Job Done!");
    }
}