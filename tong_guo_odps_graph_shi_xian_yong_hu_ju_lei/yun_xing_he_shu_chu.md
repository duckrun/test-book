## 运行和输出

准备结果表SQL如下：

create table t_kmeans_result(

cnt1 bigint,

cnt2 bigint,

cnt3 bigint,

cnt4 bigint,

cnt5 bigint,

cnt6 bigint,

cnt7 bigint,

cnt8 bigint,

cnt9 bigint,

cnt10 bigint) lifecycle 7;

在console中执行如下命令：

add jar /home/admin/duckrun/dev/open_graph_example/target/open_graph_example-0.1.jar -f;

add table t_kmeans_seed -f;

jar -resources open_graph_example-0.1.jar,t_kmeans_seed -classpath /home/admin/duckrun/dev/open_graph_example/target/open_graph_example-0.1.jar example.demo.KmeansDemo t_user_feature t_kmeans_result t_kmeans_seed;