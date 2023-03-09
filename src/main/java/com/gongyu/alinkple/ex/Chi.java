package com.gongyu.alinkple.ex;


import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.BucketizerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.ChiSquareTestBatchOp;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class Chi {
    public static void main(String[] args) throws Exception {
        List <Row> df = Arrays.asList(
                Row.of(1.1, true, 2, "A"),
                Row.of(1.1, false, 2, "B"),
                Row.of(1.1, true, 1, "B"),
                Row.of(2.2, true, 1, "A")
        );
        BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "double double, bool boolean, number int, str string");

        BatchOperator <?> bucketizer = new BucketizerBatchOp().setSelectedCols("double","number").setCutsArray(
                new double[] {1.0,2.0,2.2,4.0},new double[]{0.0,1.1});
        bucketizer.linkFrom(inOp1).print();

    }
}
