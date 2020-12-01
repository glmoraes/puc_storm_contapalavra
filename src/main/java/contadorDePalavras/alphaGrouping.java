package contadorDePalavras;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.generated.GlobalStreamId;


public class alphaGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        String word = values.get(0).toString();

        if(word.startsWith("a")) {
            boltIds.add(targetTasks.get(0));
        } else {
            boltIds.add(targetTasks.get(1));
        }
        return boltIds;
    }
}