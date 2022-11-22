package hadoop.preTraitement;

import java.util.Locale;
import java.util.Random;

public class Data{

    static String[] champs;

    public static void fromLine(String ligne)
    {
        champs = ligne.split(",");
    }

    public static int getPassangerID()
    {
        try {
            return Integer.parseInt(champs[0]);
        }catch (Exception e){
            return 0;
        }
    }

    public static int getSurvived(){

        try {
            return Integer.parseInt(champs[1]);
        }catch(Exception e){
            //System.err.println("getSurvived : "+e.getMessage());
            return 0;
        }
    }

    public static int getPclass(){

        try {
            return Integer.parseInt(champs[2]);
        }catch(Exception e){
            //System.err.println("getPclass : "+e.getMessage());
            return 0;
        }
    }

    public static String geName(){

        try {
            return (String) champs[3]+champs[4];
        }catch(Exception e){
            //System.err.println("geName : "+e.getMessage());
            return "";
        }
    }

    public static int getSex(){

        try {
            String sexStr = (String) champs[5];
            if(sexStr.equals("male")){
                return 1;
            }else if(sexStr.equals("female")){
                return 0;
            }else{
                Random r = new Random();
                int low = 0;
                int high = 1;
                int result = r.nextInt(high-low) + low;
                return result;
            }
        }catch(Exception e){
            //System.err.println("getSex : "+e.getMessage());
            Random r = new Random();
            int low = 0;
            int high = 1;
            int result = r.nextInt(high-low) + low;
            return result;
        }
    }

    public static double getAge(){

        try {
            return Double.parseDouble(champs[6]);
        }catch(Exception e){
            //System.err.println("getAge : "+e.getMessage());
            return 28.8;
        }
    }

    public static int getSibSp(){

        try {
            return Integer.parseInt(champs[7]);
        }catch(Exception e){
            //System.err.println("getSibSp : "+e.getMessage());
            return 0;
        }
    }

    public static int getParch(){

        try {
            return Integer.parseInt(champs[8]);
        }catch(Exception e){
            //System.err.println("getParch : "+e.getMessage());
            return 0;
        }
    }


    public static String getTicket(){

        try {
            return (String) champs[9];
        }catch(Exception e){
            //System.err.println("getTicket : "+e.getMessage());
            return "";
        }
    }

    public static double getFair(){

        try {
            return Double.parseDouble(champs[10]);
        }catch(Exception e){
            //System.err.println("getFair : "+e.getMessage());
            return 0.0;
        }
    }

    public static String getCabin(){

        try {
            return (String) champs[11];
        }catch(Exception e){
            //System.err.println("getCabin : "+e.getMessage());
            return "";
        }
    }

    public static String getEmbarked(){

        try {
            return (String) champs[12];
        }catch(Exception e){
            //System.err.println("getEmbarked : "+e.getMessage());
            return "";
        }
    }

}
