## 数据准备

原始数据表为tmall_user_brand，数据准备主要包括生成特征和选择初始节点。

### 生成特征 {#-0}

生成特征包括如下步骤：

1.  选择top 10 brands，生成表b
2.  统计用户购买每个品牌的次数，生成表t
3.  对表b和t进行联接，统计用户购买top 10品牌的次数，生成表ub

假设ub表数据如下：

user_id brand_id count rank

a b1 5 1

a b3 2 3

a b4 3 4

b b3 1 3

b b7 9 7

则生成的特征表如下

user_id, cnt1, … , cnt10

a 5 0 2 3 0 0 0 0 0 0

b 0 0 1 0 0 0 9 0 0 0

1.  可以通过MapReduce，对表ub和b进行联接，把表b作为资源表，在map阶段读ub表，按user_id 进行shuffle，在reduce阶段加载资源表b，读取一个key（user_id）的所有values时，按照brand顺序输出相应cnt结果（完整的MR代码见最后的gitlab地址）。当然，也可以通过UDTF实现。

这里为了代码简短，通过SQL来“补”数据，通过sum(case when…)方式实现。

完整的SQL语句如下：

create table t_user_feature as

select

user_id,

sum(case when rank=1 then cnt else 0 end) as cnt1,

sum(case when rank=2 then cnt else 0 end) as cnt2,

sum(case when rank=3 then cnt else 0 end) as cnt3,

sum(case when rank=4 then cnt else 0 end) as cnt4,

sum(case when rank=5 then cnt else 0 end) as cnt5,

sum(case when rank=6 then cnt else 0 end) as cnt6,

sum(case when rank=7 then cnt else 0 end) as cnt7,

sum(case when rank=8 then cnt else 0 end) as cnt8,

sum(case when rank=9 then cnt else 0 end) as cnt9,

sum(case when rank=10 then cnt else 0 end) as cnt10

from(

select /*+ MAPJOIN(b) */

t.user_id, t.brand_id, t.cnt, b.rank

from(

select user_id, brand_id, count(*) as cnt

from tmall_user_brand

where type='1'

group by user_id, brand_id

)t

join(

select brand_id, rank

from(

select brand_id,

row_number() over (partition by 1 order by buy_cnt desc) as rank

from(

select brand_id, count(*) as buy_cnt

from tmall_user_brand

where type='1'

group by brand_id

)t1

)t2

where t2.rank <=10

)b

on t.brand_id = b.brand_id

)ub

group by user_id;

alter table t_user_feature set lifecycle 7;

### 选择初始节点 {#-1}

对于Kmeans算法，初始节点的选取对聚类结果很重要，有很多paper研究如何选择初始节点。这里出于简单，直接随机选取3个节点，SQL如下：

drop table if exists t_kmeans_seed;

create table t_kmeans_seed as

select user_id,

cnt1,cnt2,cnt3,cnt4,cnt5,cnt6,cnt7,cnt8,cnt9,cnt10

from(

select

user_id,

cnt1,cnt2,cnt3,cnt4,cnt5,cnt6,cnt7,cnt8,cnt9,cnt10,

cluster_sample(3) over (partition by 1) as flag

from t_user_feature

)t1

where flag = true;

alter table t_kmeans_seed set lifecycle 7;