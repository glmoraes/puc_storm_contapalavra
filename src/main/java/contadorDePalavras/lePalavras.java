package contadorDePalavras;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;


public class lePalavras extends BaseRichSpout{
    private SpoutOutputCollector collector;

    private FileReader leitorArquivo;
    private BufferedReader reader;
    private boolean completo = false;

    //inicializa tudo o que é preciso e faz conexao com banco se for o caso
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.leitorArquivo = new FileReader(conf.get("arquivoDeLeitura").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Erro ao ler o arquivo "+conf.get("arquivoDeLeitura")+".");
        }
        this.reader = new BufferedReader(leitorArquivo);
    }

    //utiliza o que foi inicializado - fica pedindo novos dados, novos dados
    public void nextTuple() {
        if (!completo) {
            try {
                String palavra = reader.readLine();
                if(palavra!=null) {
                    palavra = palavra.trim();
                    palavra = palavra.toLowerCase();
                    collector.emit(new Values(palavra));
                }
                else {
                    completo = true;
                    leitorArquivo.close();
                }
            }catch (Exception e) {
                throw new RuntimeException("Erro ao ler a tupla ", e);
            }
        }

    }
    //obrigatório nos spout e bouts (mesmo que esteja  vazio) - open e nextTuple tb
    //mapeia o conteudo do emit - campos de saída da topologia
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("palavra"));
    }
}
