package contadorDePalavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TopologyFields {
    public TopologyFields() {
    }

    public static void main(String[] args) throws InterruptedException, Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lePalavra", new lePalavras());
        builder.setBolt("contaPalavra", new contaPalavras(), 2).fieldsGrouping("lePalavra", new Fields(new String[]{"palavra"}));
        Config conf = new Config();
        conf.put("arquivoDeLeitura", "/home/xxx/Documentos/Projetos/tmp_input/sample.txt");
        conf.put("diretorioResultado", "/home/xxx/Documentos/Projetos/tmp_output/");
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        try {
            System.out.println("passei aqui1");
            cluster.submitTopology("TopologiaContagemdePalavras", conf, builder.createTopology());
            System.out.println("passei aqui2");
            Thread.sleep(10000L);
            cluster.shutdown();
        } catch (Exception var5) {
        }

    }
}
