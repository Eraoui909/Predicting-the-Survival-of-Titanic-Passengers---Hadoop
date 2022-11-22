package hadoop;


import com.nhl.dflib.ColumnDataFrame;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.csv.Csv;
import com.oracle.labs.mlrg.olcut.config.ConfigurationManager;
import com.sun.xml.internal.fastinfoset.sax.Features;
import org.apache.hadoop.io.Text;
import org.tribuo.*;
import org.tribuo.classification.Label;
import org.tribuo.classification.LabelFactory;
import org.tribuo.classification.sgd.Util;
import org.tribuo.data.columnar.ColumnarIterator;
import org.tribuo.data.columnar.RowProcessor;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.dataset.DatasetView;
import org.tribuo.evaluation.TrainTestSplitter;
import org.tribuo.impl.ArrayExample;
import org.tribuo.math.la.SparseVector;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {

    static public void main(String [] args) throws IOException {

        try{
            DataFrame df = DataFrame.newFrame("PassengerId","Survived","Pclass","Sex","Age","SibSp","Parch","Fare")
                    .addRow(10000,1.0,1,1,22,1,0,6).create();

            RegModel model = new RegModel();


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
        }






    }


}
