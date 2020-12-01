package contadorDePalavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Customizado {
    public static void main(String[] args) throws InterruptedException, Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lePalavra", new lePalavras());
        builder.setBolt("contaPalavra", new contaPalavras(), 2).customGrouping("lePalavra", new alphaGrouping());

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

        } catch (Exception e) {
        }

    }
}
