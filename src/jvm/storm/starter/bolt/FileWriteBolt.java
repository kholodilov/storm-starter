package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Joiner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class FileWriteBolt extends BaseRichBolt
{

    String fileBaseName;
    BufferedWriter writer;

    public FileWriteBolt()
    {
        this("FileWriterBoltOutput");
    }

    public FileWriteBolt(String fileBaseName)
    {
        this.fileBaseName = fileBaseName;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        try
        {
            writer = Files.newBufferedWriter(
                    Paths.get(fileBaseName + System.currentTimeMillis() + ".txt"),
                    Charset.defaultCharset(),
                    StandardOpenOption.CREATE_NEW);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try
        {
            writer.write(Joiner.on(" ").join(tuple.getValues()));
            writer.newLine();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup()
    {
        try
        {
            writer.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
