package hadoop.preTraitement;



import com.nhl.dflib.*;
import com.nhl.dflib.csv.Csv;
import hadoop.RegModel;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.tribuo.DataSource;
import org.tribuo.MutableDataset;
import org.tribuo.classification.Label;
import org.tribuo.classification.LabelFactory;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.evaluation.TrainTestSplitter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TitanicReducer
        extends Reducer<Text,CustomWritable,Text,Text> {

    public TitanicReducer() {
        System.out.println("###############################################################");
        System.out.println("######################### REDUCER #############################");
        System.out.println("###############################################################");
    }

    //private IntWritable result = new IntWritable();
    private  Text mot = new Text();
    private  Text result = new Text();
    //private  CustomWritable result = new CustomWritable();

    public void reduce(Text key, Iterable<CustomWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

        RegModel model = new RegModel();
        DataFrameBuilder output = DataFrame.newFrame("id","Réel","prediction");


        for (CustomWritable val : values) {

            //System.out.println("age = \t"+val.get());

            try{
                DataFrame df = DataFrame.newFrame("PassengerId","Survived","Pclass","Sex","Age","SibSp","Parch","Fare")
                        .addRow(val.getId(),val.getSurvived(),val.getpClass(),val.getSex(),val.getParch(),val.getFair(),val.getAge(),val.getSibSp()).create();

                //System.err.println("REDUCER data = \t"+val.get());

                Csv.save(df,"tribuo/src/main/resources/dataframes/test.csv");

                LabelFactory labelFactory = new LabelFactory();
                CSVLoader<Label> csvLoader = new CSVLoader<>(labelFactory);

                DataSource<Label> titanicSource = csvLoader.loadDataSource(Paths.get("tribuo/src/main/resources/dataframes/test.csv"), "Survived");
                TrainTestSplitter<Label> titanicSplitter = new TrainTestSplitter<>(titanicSource, 0, 1L);

                MutableDataset data = new MutableDataset<>(titanicSplitter.getTest());


                //System.out.println(data.getExample(0));

                String prediction = model.predict(data.getExample(0));
                System.out.println("prediction = "+prediction);
                result.set(""+1);
                mot.set(""+val.getId());

                /*Series<String> id = Series.forData(""+val.getId());
                Series<String> survived = Series.forData(""+val.getSurvived());
                Series<String> pred = Series.forData(prediction);
                output.addColumn(""+val.getId(),id);
                output.addColumn(""+val.getSurvived(),survived);
                output.addColumn(prediction,pred);*/

                /*output.addSingleValueColumn("id",val.getId());
                output.addSingleValueColumn("survived",val.getSurvived());
                output.addSingleValueColumn("prediction",prediction);*/

                try{
                    DataFrame helper = Csv.load(Paths.get("tribuo/src/main/resources/output/predictions.csv"));
                    DataFrame test = output.foldByRow(val.getId(),val.getSurvived(),prediction);
                    DataFrame x = helper.vConcat(test);

                    Csv.save(x, Paths.get("tribuo/src/main/resources/output/predictions.csv"));
                }catch (Exception e){
                    DataFrame helper =  DataFrame.newFrame("id","Réel","prediction").empty();
                    DataFrame test = output.foldByRow(val.getId(),val.getSurvived(),prediction);
                    DataFrame x = helper.vConcat(test);

                    Csv.save(x, Paths.get("tribuo/src/main/resources/output/predictions.csv"));
                }



                //context.write(mot,result);
            }catch(Exception e){
                System.err.println("Dataframe ERROR = "+e.getMessage());
            }
            //System.out.println(output);

            /*try{
                DataFrame df = DataFrame.newFrame("PassengerId","Survived","Pclass","Sex","Age","SibSp","Parch","Fare")
                        .addRow(10000,0,1,1,22,1,0,6).create();


                Csv.save(df,"tribuo/src/main/resources/dataframes/test.csv");

                LabelFactory labelFactory = new LabelFactory();
                CSVLoader<Label> csvLoader = new CSVLoader<>(labelFactory);

                DataSource<Label> titanicSource = csvLoader.loadDataSource(Paths.get("tribuo/src/main/resources/dataframes/test.csv"), "Survived");
                TrainTestSplitter<Label> titanicSplitter = new TrainTestSplitter<>(titanicSource, 0, 1L);

                MutableDataset data = new MutableDataset<>(titanicSplitter.getTest());


                //System.out.println(data.getExample(0));

                String prediction = model.predict(data.getExample(0));
                System.out.println("prediction = "+prediction);
            }catch(Exception e){
                System.out.println("Dataframe ERROR = "+e.getMessage());
            } */

        }
    }


}
