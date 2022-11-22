package hadoop.preTraitement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CustomWritable implements Writable{

    private DoubleWritable id ;
    private DoubleWritable survived ;
    private DoubleWritable pClass ;
    private DoubleWritable age ;
    private DoubleWritable sibSp ;
    private DoubleWritable parch ;
    private DoubleWritable fair ;
    private DoubleWritable sex;

    public CustomWritable() {
        this.id         = new DoubleWritable();
        this.survived   = new DoubleWritable();
        this.pClass     = new DoubleWritable();
        this.age        = new DoubleWritable();
        this.sibSp      = new DoubleWritable();
        this.parch      = new DoubleWritable();
        this.fair       = new DoubleWritable();
        this.sex        = new DoubleWritable();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        id.write(dataOutput);
        survived.write(dataOutput);
        pClass.write(dataOutput);
        sex.write(dataOutput);
        parch.write(dataOutput);
        fair.write(dataOutput);
        age.write(dataOutput);
        sibSp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        id.readFields(dataInput);
        survived.readFields(dataInput);
        pClass.readFields(dataInput);
        sex.readFields(dataInput);
        age.readFields(dataInput);
        sibSp.readFields(dataInput);
        parch.readFields(dataInput);
        fair.readFields(dataInput);

    }

    public void  set(DoubleWritable id, DoubleWritable survived, DoubleWritable pClass, DoubleWritable sex, DoubleWritable age, DoubleWritable sibSp, DoubleWritable parch,  DoubleWritable fair) {
        this.id = id;
        this.survived = survived;
        this.pClass = pClass;
        this.sex = sex;
        this.age = age;
        this.sibSp = sibSp;
        this.parch = parch;
        this.fair = fair;
    }

    public String get() {
        return "CustomWritable{" +
                "id=" + id +
                ", survived=" + survived +
                ", pClass=" + pClass +
                ", age=" + age +
                ", sibSp=" + sibSp +
                ", parch=" + parch +
                ", fair=" + fair +
                ", sex=" + sex +
                '}';
    }

    public DoubleWritable getId() {
        return id;
    }

    public DoubleWritable getSurvived() {
        return survived;
    }

    public DoubleWritable getpClass() {
        return pClass;
    }

    public DoubleWritable getAge() {
        return age;
    }

    public DoubleWritable getSibSp() {
        return sibSp;
    }

    public DoubleWritable getParch() {
        return parch;
    }

    public DoubleWritable getFair() {
        return fair;
    }

    public DoubleWritable getSex() {
        return sex;
    }



}
