package hadoop.preTraitement;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class TitanicMapper extends Mapper<Object, Text, Text, CustomWritable>{

    Text word = new Text();
    CustomWritable data = new CustomWritable();
    IntWritable o = new IntWritable(1);

    int i = 0;

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {

        if(i != 0){

            Data.fromLine(value.toString());

            DoubleWritable id = new DoubleWritable(Data.getPassangerID());
            DoubleWritable survived = new DoubleWritable(Data.getSurvived());
            DoubleWritable pClass = new DoubleWritable(Data.getPclass());
            DoubleWritable sex = new DoubleWritable(Data.getSex());
            DoubleWritable age = new DoubleWritable(Data.getAge());
            DoubleWritable sibSp = new DoubleWritable(Data.getSibSp());
            DoubleWritable parch = new DoubleWritable(Data.getParch());
            DoubleWritable fair = new DoubleWritable(Data.getFair());


            /*if( i < 615 ){
                word.set("training");
                data.set(id,survived,pClass,sex,age,sibSp,parch,fair);
            }else{
                word.set("testing");
                data.set(id,survived,pClass,sex,age,sibSp,parch,fair);
            }*/

            word.set("data");
            data.set(id,survived,pClass,sex,age,sibSp,parch,fair);



            context.write(word, data);

            //System.out.println(id+"\t"+survived+"\t"+pClass+"\t"+name+"\t"+sex+"\t"+age+"\t"+sibSp+"\t"+parch+"\t"+ticket+"\t"+fair+"\t"+cabin+"\t"+embarked);
            //System.out.println("MAPPER age = \t"+data.getAge());

        }

        i++;
    }
}
