## 基于ODPS Graph实现Kmeans聚类 {#odps-graph-kmeans}

这里我们基于在线手册Graph示例程序的“k-均值聚类算法”来实现。先贴上代码如下（后面再解释）：

package example.demo;

public class KmeansDemo {

private final static Logger LOG = Logger.getLogger(KmeansDemo.class);

private static String RESOURCE_TABLE;

public static class KmeansVertex extends

Vertex<Text, Tuple, NullWritable, NullWritable> {

@Override

public void compute(

ComputeContext<Text, Tuple, NullWritable, NullWritable> context,

Iterable&lt;NullWritable&gt; messages) throws IOException {

context.aggregate(this.getValue());

}

}

public static class KmeansVertexReader extends

GraphLoader<Text, Tuple, NullWritable, NullWritable> {

@Override

public void load(LongWritable recordNum, WritableRecord record,

MutationContext<Text, Tuple, NullWritable, NullWritable> context)

throws IOException {

Tuple val = new Tuple();

for(int i=1; i<record.size(); ++i) {

val.append(record.get(i));

}

KmeansVertex vertex = new KmeansVertex();

vertex.setId(new Text(String.valueOf(record.get(0))));

vertex.setValue(val);

context.addVertexRequest(vertex);

}

}

public static class KmeansAggrValue implements Writable {

Tuple centers = new Tuple();

Tuple sums = new Tuple();

Tuple counts = new Tuple();

@Override

public void write(DataOutput out) throws IOException {

centers.write(out);

sums.write(out);

counts.write(out);

}

@Override

public void readFields(DataInput in) throws IOException {

centers = new Tuple();

centers.readFields(in);

sums = new Tuple();

sums.readFields(in);

counts = new Tuple();

counts.readFields(in);

}

@Override

public String toString() {

return "centers " + centers.toString() + ", sums " + sums.toString()

+ ", counts " + counts.toString();

}

}

public static class KmeansAggregator extends Aggregator&lt;KmeansAggrValue&gt; {

@Override

public KmeansAggrValue createStartupValue(WorkerContext context)

throws IOException {

KmeansAggrValue aggrVal = null;

aggrVal = new KmeansAggrValue();

aggrVal.centers = new Tuple();

aggrVal.sums = new Tuple();

aggrVal.counts = new Tuple();

RESOURCE_TABLE = context.getConfiguration().get("RESOURCE_TABLE");

Iterable&lt;WritableRecord&gt; iter = context.readResourceTable(RESOURCE_TABLE);

for(WritableRecord record : iter) {

Tuple center = new Tuple();

Tuple sum = new Tuple();

for (int i = 1; i < record.size(); ++i) {

center.append(record.get(i));

sum.append(new LongWritable(0L));

}

LongWritable count = new LongWritable(0L);

aggrVal.sums.append(sum);

aggrVal.counts.append(count);

aggrVal.centers.append(center);

}

return aggrVal;

}

@Override

public KmeansAggrValue createInitialValue(WorkerContext context)

throws IOException {

return (KmeansAggrValue) context.getLastAggregatedValue(0);

}

@Override

public void aggregate(KmeansAggrValue value, Object item) {

int min = 0;

long mindist = Long.MAX_VALUE;

Tuple point = (Tuple) item;

for (int i = 0; i < value.centers.size(); i++) {

Tuple center = (Tuple) value.centers.get(i);

// use Euclidean Distance, no need to calculate sqrt

long dist = 0L;

for (int j = 0; j < center.size(); j++) {

long v = ((LongWritable) point.get(j)).get()

- ((LongWritable) center.get(j)).get();

dist += v * v;

}

if (dist < mindist) {

mindist = dist;

min = i;

}

}

// update sum and count

Tuple sum = (Tuple) value.sums.get(min);

for (int i = 0; i < point.size(); i++) {

LongWritable s = (LongWritable) sum.get(i);

s.set(s.get() + ((LongWritable) point.get(i)).get());

}

LongWritable count = (LongWritable) value.counts.get(min);

count.set(count.get() + 1L);

}

@Override

public void merge(KmeansAggrValue value, KmeansAggrValue partial) {

for (int i = 0; i < value.sums.size(); i++) {

Tuple sum = (Tuple) value.sums.get(i);

Tuple that = (Tuple) partial.sums.get(i);

for (int j = 0; j < sum.size(); j++) {

LongWritable s = (LongWritable) sum.get(j);

s.set(s.get() + ((LongWritable) that.get(j)).get());

}

}

for (int i = 0; i < value.counts.size(); i++) {

LongWritable count = (LongWritable) value.counts.get(i);

count.set(count.get() + ((LongWritable) partial.counts.get(i)).get());

}

}

@SuppressWarnings("rawtypes")

@Override

public boolean terminate(WorkerContext context, KmeansAggrValue value)

throws IOException {

// compute new centers

Tuple newCenters = new Tuple(value.sums.size());

for (int i = 0; i < value.sums.size(); i++) {

Tuple sum = (Tuple) value.sums.get(i);

Tuple newCenter = new Tuple(sum.size());

LongWritable c = (LongWritable) value.counts.get(i);

if(c.equals(0L)) {

continue;

}

for (int j = 0; j < sum.size(); j++) {

LongWritable s = (LongWritable) sum.get(j);

newCenter.set(j, new LongWritable(new Double((double)s.get()/ c.get()+0.5).longValue()));

// reset sum for next iteration

s.set(0L);

}

// reset count for next iteration

c.set(0L);

newCenters.set(i, newCenter);

}

// update centers

Tuple oldCenters = value.centers;

value.centers = newCenters;

LOG.info("old centers: " + oldCenters + ", new centers: " + newCenters);

// compare new/old centers

boolean converged = true;

for (int i = 0; i < value.centers.size() && converged; i++) {

Tuple oldCenter = (Tuple) oldCenters.get(i);

Tuple newCenter = (Tuple) newCenters.get(i);

long sum = 0L;

for (int j = 0; j < newCenter.size(); j++) {

long v = ((LongWritable) newCenter.get(j)).get()

- ((LongWritable) oldCenter.get(j)).get();

sum += v * v;

}

double dist = Math.sqrt(sum);

LOG.info("old center: " + oldCenter + ", new center: " + newCenter

+ ", dist: " + dist);

// converge threshold for each center: 0.05

converged = dist < 0.05d;

}

if (converged || context.getSuperstep() == context.getMaxIteration() - 1) {

// converged or reach max iteration, output centers

for (int i = 0; i < value.centers.size(); i++) {

context.write(((Tuple) value.centers.get(i)).toArray());

}

// true means to terminate iteration

return true;

}

// false means to continue iteration

return false;

}

}

private static void printUsage() {

System.out.println("Usage: &lt;in&gt; &lt;out&gt; &lt;resource&gt; [Max iterations (default 30)]");

System.exit(-1);

}

public static void main(String[] args) throws IOException {

if (args.length < 3)

printUsage();

GraphJob job = new GraphJob();

job.setGraphLoaderClass(KmeansVertexReader.class);

job.setRuntimePartitioning(false);

job.setVertexClass(KmeansVertex.class);

job.setAggregatorClass(KmeansAggregator.class);

job.addInput(TableInfo.builder().tableName(args[0]).build());

job.addOutput(TableInfo.builder().tableName(args[1]).build());

job.set("RESOURCE_TABLE", args[2]);

// default max iteration is 30

job.setMaxIteration(30);

if (args.length >= 4)

job.setMaxIteration(Integer.parseInt(args[3]));

long start = System.currentTimeMillis();

job.run();

System.out.println("Job Finished in "

+ (System.currentTimeMillis() - start) / 1000.0 + " seconds");

}

}

和MapReduce编程框架类似，在main函数，先实例化一个GraphJob，对job设置后，通过job.run()提交。

KmeansVertexReader类实现加载图，定义图节点。由于kmeans算法是计算节点距离，因此不需要定义边；此外它需要对迭代结果进行汇总，所以通过KmeansAggregator继承Aggregator，实现每一步迭代计算。