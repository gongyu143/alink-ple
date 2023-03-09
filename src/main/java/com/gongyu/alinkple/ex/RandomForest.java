package com.gongyu.alinkple.ex;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestModelInfoBatchOp;
import com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RandomForest {

    public static void main(String[] args) throws Exception {
        List<Row> df = Arrays.asList(
                Row.of(1.0, "A", 0, 0, 0),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1),
                Row.of(2.0, "B", 1, 1, 0),
                Row.of(3.0, "C", 2, 2, 1),
                Row.of(4.0, "D", 3, 3, 1)
        );
        BatchOperator<?> batchSource = new MemSourceBatchOp(df, " f0 double, f1 string, f2 int, f3 int, label int");

        BatchOperator<?> trainOp = new RandomForestRegTrainBatchOp()
                .setLabelCol("label")
                .setFeatureCols("f0", "f1", "f2", "f3")
                .linkFrom(batchSource);


        String model = new RandomForest().model(trainOp);

        String imagePath = "D:/alink/b";
        new AkSourceBatchOp()
                .setFilePath(model)
                .link(
                        new RandomForestModelInfoBatchOp()
                                .lazyPrintModelInfo()
                                .lazyCollectModelInfo(e -> {
                                    try {
                                        e.saveTreeAsImage(imagePath + "/tree1.png", 0, true);
                                    } catch (IOException ex) {
                                        ex.printStackTrace();
                                    }
                                })
                );
        BatchOperator.execute();
    }

    String model( BatchOperator<?> trainOp) throws Exception {

        String modelPath = "D:\\alink\\b\\123342";
        new AkSinkBatchOp()
                .setFilePath(modelPath)
                .linkFrom(trainOp);
        BatchOperator.execute();
        return modelPath;
    }
}
