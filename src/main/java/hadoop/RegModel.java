package hadoop;


import java.io.IOException;
import java.nio.file.Paths;
import org.tribuo.*;
import org.tribuo.classification.evaluation.LabelEvaluation;
import org.tribuo.classification.evaluation.LabelEvaluator;
import org.tribuo.classification.sgd.linear.LogisticRegressionTrainer;
import org.tribuo.evaluation.TrainTestSplitter;
import org.tribuo.data.csv.CSVLoader;
import org.tribuo.classification.*;

public class RegModel {

    LabelFactory labelFactory = new LabelFactory();
    CSVLoader<Label> csvLoader = new CSVLoader<>(labelFactory);

    Model<Label> titanicModel;
    Trainer<Label> trainer;

    MutableDataset trainingDataset;
    MutableDataset testingDataset;

    public RegModel() throws IOException {


        //String[] titanicHeaders = new String[]{"sepalLength", "sepalWidth", "petalLength", "petalWidth", "species"};
        DataSource<Label> titanicSource = csvLoader.loadDataSource(Paths.get("C:\\Users\\HAMZA ERAOUI\\datasets\\titanicPrepared.csv"), "Survived");
        TrainTestSplitter<Label> titanicSplitter = new TrainTestSplitter<>(titanicSource, 0.7, 1L);

        trainingDataset = new MutableDataset<>(titanicSplitter.getTrain());
        testingDataset = new MutableDataset<>(titanicSplitter.getTest());

        //System.out.println(String.format("Training data size = %d, number of features = %d, number of classes = %d",trainingDataset.size(),trainingDataset.getFeatureMap().size(),trainingDataset.getOutputInfo().size()));
        //System.out.println(String.format("Testing data size = %d, number of features = %d, number of classes = %d",testingDataset.size(),testingDataset.getFeatureMap().size(),testingDataset.getOutputInfo().size()));

        trainer = new LogisticRegressionTrainer();
        //System.out.println(trainer.toString());

        titanicModel = trainer.train(trainingDataset);

        LabelEvaluator evaluator = new LabelEvaluator();
        LabelEvaluation evaluation = evaluator.evaluate(titanicModel, testingDataset);
        System.out.println(evaluation.toString());


        //System.out.println(evaluation.getConfusionMatrix().toString());



    }

    public String predict(Example<Label> example){
        System.out.println(example);
        Prediction<Label> prediction = titanicModel.predict(example);
        //System.out.println("output = \t"+prediction.getOutput());

        return prediction.getOutput().getLabel();
    }

}
