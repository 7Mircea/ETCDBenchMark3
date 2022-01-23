package benchmark;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.GetOption;

import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import java.util.concurrent.ExecutionException;

import io.etcd.jetcd.kv.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

class Benchmark {
    static final int[] scaleFactorArr = new int[] { 10_000,100_000,200_000,400_000,405_184 };
    static final int repeatNr = 10;
    static final int nrKeys = 405_184;

    public static void main(String[] args) {
        try {
            // create client
            Client client = Client.builder().endpoints("http://localhost:2379").build();
            KV kvClient = client.getKVClient();

            BufferedReader reader = new BufferedReader(
                    new FileReader("/home/mircea/CTI/AN1/SEM1/SABD/PROIECT/iot_telemetry_data.csv"));

            String line = null;
            int nrLines = 0;

            String[] charKeys = new String[nrKeys];
            String[] charValues = new String[nrKeys];
            int end = 0, start = 0;
            line = reader.readLine();// first line contains description not actual values
            while ((line = reader.readLine()) != null && nrLines < nrKeys) {
                charValues[nrLines] = line;
                charKeys[nrLines] = String.valueOf(++nrLines);
            }
            System.out.println("nr lines: " + nrLines);
            reader.close();
            String[] charKeysCopy = null;
            String[] charValuesCopy = null;
            ByteSequence[] keySeqArr = null; // key sequentaly array
            ByteSequence[] valSeqArr = null; // values sequentaly array
            

            for (int i = 0; i < scaleFactorArr.length; ++i) {
                charKeysCopy = Arrays.copyOfRange(charKeys,0,scaleFactorArr[i]);
                charValuesCopy = Arrays.copyOfRange(charValues,0,scaleFactorArr[i]);
                keySeqArr = getByteSequence(charKeysCopy); //key sequentaly array
                valSeqArr = getByteSequence(charValuesCopy); //values sequentaly array
                double average;
                double[] durationArr = new double[repeatNr];
                System.out.println("Scalation factor : " + scaleFactorArr[i]);
                for (int j = 0; j < repeatNr; ++j) {
                    long startTime = System.currentTimeMillis();
                    put(kvClient, keySeqArr, valSeqArr);
                    long endTime = System.currentTimeMillis();

                    durationArr[j] = (double)(endTime - startTime);
                    System.out.println("Inseration time:"+ durationArr[j] + "s");

                    delete(kvClient,keySeqArr);
                }

                //computing average
                printMeanAndStddev(durationArr);
            }

            charKeysCopy = Arrays.copyOfRange(charKeys, scaleFactorArr.length-1, scaleFactorArr[scaleFactorArr.length-1]);
            charValuesCopy = Arrays.copyOfRange(charValues, scaleFactorArr.length-1, scaleFactorArr[scaleFactorArr.length-1]);
            keySeqArr = getByteSequence(charKeysCopy); // key sequentaly array
            valSeqArr = getByteSequence(charValuesCopy); // values sequentaly array
            put(kvClient, keySeqArr, valSeqArr);

            getValue(kvClient, ByteSequence.from(String.valueOf(2).getBytes()));// first operation takes a lot of time
            // compared to the next and makes the stddev to be greater than the mean, so we
            // need a warm up operation
            // operations
            runMultipleTimes(kvClient, keySeqArr, valSeqArr);

            // delete(kvClient, keySeqArr);

            // // get the CompletableFuture
            // CompletableFuture<GetResponse> getFuture = kvClient.get(key);

            // // get the value from CompletableFuture
            // GetResponse response = getFuture.get();

            // // delete the key
            // kvClient.delete(key).get();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("opreste");
    }

    public static int getIndex(String line) {
        int i = 0;
        for (; i < line.length(); ++i) {
            if (line.charAt(i) == ',')
                break;
        }
        return i >= line.length() ? -1 : i;
    }

    /**
     * returns the byte sequence from a String array
     * 
     * @param charArr the String array to be converted to ByteSequence
     * @return the ByteSequence
     */
    public static ByteSequence[] getByteSequence(String[] charArr) {
        ByteSequence[] arr = new ByteSequence[charArr.length];
        int i = 0;
        for (String key : charArr) {
            arr[i++] = ByteSequence.from(key.getBytes());
        }
        return arr;
    }

    /**
     * inserts the given keys and values in the database. It's a blocking,
     * synchronous call.
     * 
     * @param kvClient the client class that interacts with the database
     * @param keys     the keys to be inserted
     * @param values   the values to be inserted
     */
    public static void put(KV kvClient, ByteSequence[] keys, ByteSequence[] values) {
        if (keys.length != values.length) {
            System.out.println("eroare in put. Lungimi diferite chei si valori");
            return;
        }
        int i = 0;
        try {
            for (; i < keys.length; ++i) {
                kvClient.put(keys[i], values[i]).get();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * deletes the entries that coresponds to the given keys
     * 
     * @param kvClient the client class that interacts with the database
     * @param keys     the keys to be deleted
     */
    public static void delete(KV kvClient, ByteSequence[] keys) {
        try {
            int i = 0;
            for (ByteSequence key : keys) {
                kvClient.delete(key).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void runMultipleTimes(KV kvClient, ByteSequence[] keySeqArr, ByteSequence[] valSeqArr) {
        double average;
        // init
        double[] durationArr = new double[repeatNr];

        // operations
        // update
        System.out.println("update");
        for (int j = 0; j < repeatNr; ++j) {
        ByteSequence keySeq = ByteSequence.from(String.valueOf(7).getBytes());
        durationArr[j] = updateKey(kvClient, keySeq, 25);
        }
        printMeanAndStddev(durationArr);

        System.out.println("delete");
        for (int j = 0; j < repeatNr; ++j) {
        ByteSequence keySeq = ByteSequence.from(String.valueOf(10).getBytes());
        String value = getValue(kvClient, keySeq);
        durationArr[j] = deleteKey(kvClient, keySeq);
        putValue(kvClient, keySeq, ByteSequence.from(value.getBytes()));
        }
        printMeanAndStddev(durationArr);

        System.out.println("select 1");
        for (int j = 0; j < repeatNr; ++j) {
        durationArr[j] = select1(kvClient, 50., 60.);
        }
        printMeanAndStddev(durationArr);

        System.out.println("select 2");
        for (int j = 0; j < repeatNr; ++j) {
        durationArr[j] = select2(kvClient, true);
        }
        printMeanAndStddev(durationArr);

        System.out.println("select 3");
        for (int j = 0; j < repeatNr; ++j) {
        durationArr[j] = select3(kvClient);
        }
        printMeanAndStddev(durationArr);

        // System.out.println("select 4");
        // for (int j = 0; j < repeatNr; ++j) {
        //     durationArr[j] = select4(kvClient);
        // }
        // printMeanAndStddev(durationArr);

    }

    public static void printMeanAndStddev(double[] durationArr) {
        // computing average
        double sum = 0;
        for (int j = 0; j < repeatNr; ++j) {
            sum += durationArr[j];
            System.out.println(
                    "time:" + durationArr[j] + " ms.");
        }
        double average = sum / (double) repeatNr;

        // computing standard deviation
        sum = 0;
        for (int j = 0; j < repeatNr; ++j) {
            double diff = average - durationArr[j];
            sum += diff * diff;
        }
        double stddev = Math.sqrt(1.0 / (double) repeatNr * sum);
        System.out.println("Mean : " + average + " ms. Std dev: " + stddev + "\n");
    }

    /**
     * updates the temperature and returns the time needed for the modification
     * 
     * @param kvClient       the client class to interact with the ETCD database
     * @param key            the key for the temperature
     * @param newTemperature the new temperature
     * @return the time needed for the operation
     */
    public static double updateKey(KV kvClient, ByteSequence key, double newTemperature) {
        double duration = 0.;
        try {
            long startTime = System.currentTimeMillis();

            // get the CompletableFuture
            CompletableFuture<GetResponse> getFuture = kvClient.get(key);
            // get the value from CompletableFuture
            GetResponse response = getFuture.get();
            List<KeyValue> list = response.getKvs();
            KeyValue keyValue = list.get(0);
            ByteSequence seq = keyValue.getValue();
            String value = seq.toString();
            String[] arr = value.split(",");
            if (arr.length == 9)
                arr[8] = "\"" + String.valueOf(newTemperature) + "\"";
            StringBuilder builder = new StringBuilder();
            for (String el : arr) {
                builder.append(el).append(",");
            }
            builder.deleteCharAt(builder.length() - 1);
            String newValue = builder.toString();
            kvClient.put(key, ByteSequence.from(newValue.getBytes())).get();

            long endTime = System.currentTimeMillis();
            duration = (double) (endTime - startTime);
        } catch (InterruptedException | ExecutionException | IndexOutOfBoundsException e) {
            e.printStackTrace();
            return -1.0;
        }
        return duration;
    }

    /**
     * delete the given entry(key value pair) that coresponds to this key and
     * measures the time needed for this operation
     * 
     * @param kvClient the client class that interacts with the database
     * @param key      the key to be deleted
     * @return the duration of the delete operation if it succeds or -1 if an
     *         exception arises
     */
    public static double deleteKey(KV kvClient, ByteSequence key) {
        double duration = 0.;
        try {
            long startTime = System.currentTimeMillis();

            kvClient.delete(key).get();

            long endTime = System.currentTimeMillis();
            duration = (double) (endTime - startTime);
            return duration;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * gets the value coresponding to the key that's sent as a parameter
     * 
     * @param kvClient the client class that interacts with the database
     * @param key      the key of the value that will be retrieved by this function
     * @return the value that coresponds to the parameter key if the value is in the
     *         database, "" if there isn't any key and
     *         "error" if InterruptedException or ExecutionExeption arises
     */
    public static String getValue(KV kvClient, ByteSequence key) {
        try {
            CompletableFuture<GetResponse> future = kvClient.get(key);
            GetResponse response = future.get();
            List<KeyValue> entries = response.getKvs();
            if (entries.isEmpty())
                return "";
            KeyValue entry = entries.get(0);
            ByteSequence valueBS = entry.getValue();
            return valueBS.toString();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return "error";
        }
    }

    /**
     * puts only one key value pair in the database
     * 
     * @param kvClient the client class that interacts with the database
     * @param key      the key from the key value pair to be inserted
     * @param value    the value from the key value pair to be inserted
     */
    public static void putValue(KV kvClient, ByteSequence key, ByteSequence value) {
        try {
            kvClient.put(key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simultes selects similar to select 1: select * from table_name where humidity
     * between 50 and 60;
     * 
     * @param humidityStart in this case is 50
     * @param humidityEnd   in this case is 60
     */
    // public static double select1(KV kvClient, double humidityStart, double
    // humidityEnd) {
    // double duration = 0.;
    // try {
    // long startTime = System.currentTimeMillis();
    // CompletableFuture<GetResponse> future =
    // kvClient.get(ByteSequence.from(String.valueOf(0).getBytes()),
    // GetOption.newBuilder().withRange(ByteSequence.from(String.valueOf(nrKeys).getBytes()))
    // .withCountOnly(true).build());

    // GetResponse response = future.get();
    // List<KeyValue> list = response.getKvs();
    // System.out.println("extracted list" + list.size());
    // if (list.size() != nrKeys) {
    // System.out.println("Nu au fost extrase toate cheile");
    // }
    // BlockingQueue<KeyValue> queue = new ArrayBlockingQueue<KeyValue>(list.size(),
    // false, list);
    // list.clear();
    // // work
    // // first part(this case 0 - aprox 100_000)
    // CompletableFuture<Integer> future1 = CompletableFuture.supplyAsync(() -> {
    // while (true) {

    // }
    // Iterator<KeyValue> iterator = queue.iterator();
    // int limit = list.size() / 4, i = 0;
    // while (iterator.hasNext()) {
    // KeyValue entry = iterator.next();
    // ByteSequence valueBS = entry.getValue();
    // String value = valueBS.toString();
    // StringBuilder builder = new StringBuilder();
    // if (checkHumidityValue(value, humidityStart, humidityEnd))
    // builder.append(value).append("\n");
    // System.out.println(builder.toString());
    // ++i;
    // }
    // return 1;
    // });
    // // second part (this case aprox 100_000 - aprox 200_000)
    // CompletableFuture<Integer> future2 = CompletableFuture.supplyAsync(() -> {
    // Iterator<KeyValue> iterator = queue.iterator();
    // int limit = list.size() / 4, i = 0;
    // while (iterator.hasNext() && i < limit)
    // ;
    // while (iterator.hasNext() && i < 2 * limit) {
    // KeyValue entry = iterator.next();
    // ByteSequence valueBS = entry.getValue();
    // String value = valueBS.toString();
    // StringBuilder builder = new StringBuilder();
    // if (checkHumidityValue(value, humidityStart, humidityEnd))
    // builder.append(value).append("\n");
    // System.out.println(builder.toString());
    // ++i;
    // }
    // return 1;
    // });
    // CompletableFuture<Integer> future3 = CompletableFuture.supplyAsync(() -> {
    // Iterator<KeyValue> iterator = queue.iterator();
    // int limit = list.size() / 4, i = 0;
    // while (iterator.hasNext() && i < 2 * limit)
    // ;
    // while (iterator.hasNext() && i < limit) {
    // KeyValue entry = iterator.next();
    // ByteSequence valueBS = entry.getValue();
    // String value = valueBS.toString();
    // StringBuilder builder = new StringBuilder();
    // if (checkHumidityValue(value, humidityStart, humidityEnd))
    // builder.append(value).append("\n");
    // System.out.println(builder.toString());
    // ++i;
    // }
    // return 1;
    // });
    // CompletableFuture<Integer> future4 = CompletableFuture.supplyAsync(() -> {
    // Iterator<KeyValue> iterator = queue.iterator();
    // int limit = list.size() / 4, i = 0;
    // while (iterator.hasNext() && i < 3 * limit)
    // ;
    // while (iterator.hasNext() && i < limit) {
    // KeyValue entry = iterator.next();
    // ByteSequence valueBS = entry.getValue();
    // String value = valueBS.toString();
    // StringBuilder builder = new StringBuilder();
    // if (checkHumidityValue(value, humidityStart, humidityEnd))
    // builder.append(value).append("\n");
    // System.out.println(builder.toString());
    // ++i;
    // }
    // return 1;
    // });

    // CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(future1,
    // future2, future3, future4);
    // try {
    // combinedFuture.get();
    // } catch (InterruptedException | ExecutionException e) {
    // e.printStackTrace();
    // }

    // long endTime = System.currentTimeMillis();
    // duration = (double) (endTime - startTime);
    // return duration;
    // } catch (InterruptedException | ExecutionException e) {
    // e.printStackTrace();
    // return -1;
    // }
    // }

    public static double select1(KV kvClient, double humidityStart, double humidityEnd) {
        double duration = 0.;

        long startTime = System.currentTimeMillis();

        BlockingQueue<String> bQueue = new ArrayBlockingQueue(nrKeys, false);
        for (int i = 0; i < nrKeys; ++i) {
            String value = getValue(kvClient, ByteSequence.from(String.valueOf(i).getBytes()));
            bQueue.add(value);
        }
        Iterator<String> it = bQueue.iterator();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (it.hasNext()) {
            String value = it.next();
            if (checkHumidityValue(value, humidityStart, humidityEnd))
                builder.append(value).append("\n");
            ++i;
        }
        System.out.println(builder.toString());

        long endTime = System.currentTimeMillis();
        duration = (double) (endTime - startTime);
        return duration;

    }

    /**
     * get values as String from KeyValue list
     * 
     * @param list
     * @return
     */
    public static List<String> getValues(List<KeyValue> list) {
        List<String> returnList = new ArrayList<String>(list.size());
        Iterator<KeyValue> iterator = list.iterator();
        while (iterator.hasNext()) {
            returnList.add(iterator.next().getValue().toString());
        }
        return returnList;
    }

    /**
     * get keys as String from KeyValue list
     * 
     * @param list
     * @return
     */
    public static List<String> getKeys(List<KeyValue> list) {
        List<String> returnList = new ArrayList<String>(list.size());
        Iterator<KeyValue> iterator = list.iterator();
        while (iterator.hasNext()) {
            returnList.add(iterator.next().getKey().toString());
        }
        return returnList;
    }

    // private static class Consumer implements Runnable {
    // BlockingQueue<KeyValue> queue;
    // Boolean terminated = false;

    // Consumer(BlockingQueue queue) {
    // this.queue = queue;
    // }

    // public void terminate() {
    // this.terminated = true;
    // }

    // @Override
    // public void run() {
    // while (true) {
    // KeyValue entry = queue.take();
    // ByteSequence valueBS = entry.getValue();
    // String value = valueBS.toString();
    // StringBuilder builder = new StringBuilder();
    // if (checkHumidityValue(value, humidityStart, humidityEnd))
    // builder.append(value).append("\n");
    // if (terminated)
    // break;
    // }
    // System.out.println(builder.toString());
    // }

    // }

    /**
     * checks the fourth column from the String, because humidity is the fourth
     * value
     * 
     * @param value      the value that is checked for humidity value
     * @param limitStart the minimum value for the humidity
     * @param limitEnd   the maximum value for the humidity
     * @return true if humidity from value parameter is between limitStart and
     *         limitEnd parameter.
     *         If it does not exist or isn't between the limits it return false
     */
    public static boolean checkHumidityValue(String value, double limitStart, double limitEnd) {
        try {
            String[] array = value.split(",");
            if (array.length < 4)
                return false;
            StringBuilder builder = new StringBuilder(array[3]);
            builder.deleteCharAt(0).deleteCharAt(builder.length() - 1);
            double humidity = Double.valueOf(builder.toString());
            if (humidity > limitStart && humidity < limitEnd)
                return true;
            else
                return false;
        } catch (NumberFormatException e) {
            System.out.println(e.getMessage());
            return false;
        }

    }

    /**
     * select 2: select * from table_name where light is TRUE;
     * 
     * @param kvClient
     * @param light
     * @return
     */
    public static double select2(KV kvClient, boolean light) {
        double duration = 0.;

        long startTime = System.currentTimeMillis();

        BlockingQueue<String> bQueue = new ArrayBlockingQueue(nrKeys, false);
        for (int i = 0; i < nrKeys; ++i) {
            String value = getValue(kvClient, ByteSequence.from(String.valueOf(i).getBytes()));
            bQueue.add(value);
        }
        Iterator<String> it = bQueue.iterator();
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (it.hasNext()) {
            String value = it.next();
            if (checkLight(value, light))
                builder.append(value).append("\n");
            ++i;
        }
        System.out.println(builder.toString());

        long endTime = System.currentTimeMillis();
        duration = (double) (endTime - startTime);
        return duration;

    }

    /**
     * checks the fifth column from the String, because light is the fourth
     * value
     * 
     * @param value the value that is checked for light value
     * @param light if light is on or off
     * @return true if light column from value parameter is equal to light parameter
     *         If it does not exist or hasn't the same value as light paramter it
     *         return false
     */
    public static boolean checkLight(String value, boolean light) {
        try {
            String[] array = value.split(",");
            if (array.length < 5)
                return false;
            StringBuilder builder = new StringBuilder(array[4]);
            builder.deleteCharAt(0).deleteCharAt(builder.length() - 1);
            boolean lightFromDB = Boolean.valueOf(builder.toString());
            if (lightFromDB == light)
                return true;
            else
                return false;
        } catch (NumberFormatException e) {
            System.out.println(e.getMessage());
            return false;
        }

    }

    /**
     * select 3: select count(keys) from table_name;
     * 
     * @param kvClient
     * @param light
     * @return
     */
    public static double select3(KV kvClient) {
        double duration = 0.;

        long startTime = System.currentTimeMillis();

        int i = 1;
        while (true) {
            String value = getValue(kvClient, ByteSequence.from(String.valueOf(i).getBytes()));
            if (value.equals(""))
                break;
            ++i;
        }
        System.out.println("Key number " + (--i));

        long endTime = System.currentTimeMillis();
        duration = (double) (endTime - startTime);
        return duration;

    }

    /**
     * select 4: select min(temperature) from table_name;
     * 
     * @param kvClient
     * @param light
     * @return
     */
    public static double select4(KV kvClient) {
        double duration = 0.;

        long startTime = System.currentTimeMillis();

        int i = 1;
        double minTemperature = Double.MAX_VALUE;
        while (true) {
            String value = getValue(kvClient, ByteSequence.from(String.valueOf(i).getBytes()));
            if (value.equals(""))
                break;
            double temperature = getTemperature(value);
            minTemperature = minTemperature < temperature ? minTemperature : temperature;
            ++i;
        }
        System.out.println("Min temperature is " + minTemperature);

        long endTime = System.currentTimeMillis();
        duration = (double) (endTime - startTime);
        return duration;

    }

    public static double getTemperature(String entry) {
        try {
            String[] array = entry.split(",");
            if (array.length < 9)
                return -1.0;
            StringBuilder builder = new StringBuilder(array[8]);
            builder.deleteCharAt(0).deleteCharAt(builder.length() - 1);
            Double temperature = Double.valueOf(builder.toString());
            return temperature;
        } catch (NumberFormatException e) {
            System.out.println(e.getMessage());
            return -1.0;
        }
    }
}
