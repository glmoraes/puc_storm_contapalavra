package contadorDePalavras;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class contaPalavras extends BaseBasicBolt {
    Map<String, Integer> contagem;
    Integer Id;
    String nome;
    String nomeArquivo;

   public void prepare(Map stormConf, TopologyContext context) {
        this.contagem = new HashMap<String, Integer>();
        this.nome = context.getThisComponentId();
        this.Id = context.getThisTaskId();
        this.nomeArquivo = stormConf.get("diretorioResultado").toString() + "saida-" + this.nome + this.Id + ".txt";
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String palavra = input.getString(0);
        if (!this.contagem.containsKey(palavra)) {
            this.contagem.put(palavra, 1);
        } else {
            Integer contador = (Integer)this.contagem.get(palavra) + 1;
            this.contagem.put(palavra, contador);
        }

    }

    public void cleanup() {
        try {
            PrintWriter writer = new PrintWriter(this.nomeArquivo, "UTF-8");

            for(Map.Entry<String,Integer> entry: contagem.entrySet()) {
                writer.println(entry.getKey() + ": " + entry.getValue());
            }

            writer.close();
        } catch (Exception e) {
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
