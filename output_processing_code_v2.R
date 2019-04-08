setwd("E:/Users/Zuo/output/")
library(dplyr); library(tidyr); library(readxl); library(lubridate)
library(data.table)
library(fasttime)
#require(bit64)
options(scipen = 200)
library(ROCR)

target <- TRUE
level <- 'day'

impression_digital_map_path <- 
  "./impression_digital_map.csv"

# - normal file path
impression_digital_path <- 
  "./impression_digital.csv"
non_media_with_seasonality_path <- 
  "./non_media_with_seasonality.csv"


#source('./CODE - MTA Data Processing Functions - UTF8-REFORMATTED-REINDENTED-AUDITING - Nestle.R', encoding = "UTF-8")
transformed_adstock_path <- 
  "./bp_depth/transformed_adstock_avg_max.csv"
bma_output_path <- 
  "./bp_depth/bma_output_max.csv"
bma_models_final_path <- 
  "./bma_models_final.csv"
NA_Strings <- c("NA", "Na", 'na', "NULL", "Null", "null", "")

model_input <- fread(
  input = transformed_adstock_path, sep = ",", header = TRUE, 
  encoding = "UTF-8", data.table = FALSE, colClasses = "character", 
  na.strings = NA_Strings)
colnames(model_input) <-
  gsub('transformed_adstock.', "", colnames(model_input))
model_input$V1 <- NULL
model_input$data_retention <- NULL
model_input_backup <- model_input
non_digital_cols <- names(model_input)[!grepl("^digital_", names(model_input), ignore.case = TRUE)]
digital_cols <- names(model_input)[grepl("^digital_", names(model_input), ignore.case = TRUE)]
model_input <- dplyr::select_at(model_input, .vars = unique(c("id","date",non_digital_cols, "digital_exposure", digital_cols)))
#col_names <- tibble(old = names(model_input)) %>% 
# mutate(new = ifelse(test = grepl("digital_", old) | grepl("paidsearch_", old) | grepl("tv_", old), 
#                    yes = old, no = paste0("data_", old))) %>% as.data.table()
#setnames(model_input, old = col_names$old, new = col_names$new)
#colnames(model_input)[1] <- 'id'
#colnames(model_input)[2] <- 'date'

model_input$id <- gsub(",", '', model_input$id)
model_input$id <- as.numeric(model_input$id)
model_input$date <- as.Date(model_input$date)
model_input$data_target <- as.integer(model_input$data_target) # 将target转换为整形
digital_vars <- grep(pattern = "digital_", x = names(model_input), value = TRUE)
model_input[, digital_vars] <- sapply(X = model_input[, digital_vars], FUN = as.numeric)
rm(digital_vars)
data_vars <- grep(pattern = "data_", x = names(model_input), value = TRUE)
model_input[, data_vars] <- sapply(X = model_input[, data_vars], FUN = as.numeric)
rm(data_vars)
model_input <-
  select(model_input,
         id,
         date,
         grep('data_', colnames(model_input)),
         grep('digital_', colnames(model_input)))
if (target == TRUE) {
  model_input <-
    filter(model_input, data_target == 1)
} else if (target == FALSE) {
  model_input <- filter(model_input, data_target == 0)}
#hardcode补充存在impression_digital_map中但不在transformed_adstock中的触点
model_input$digital_k003 <-0
model_input$digital_k004 <-0
model_input$digital_d025 <-0
model_input$digital_d037 <-0
model_input$digital_d039 <-0
model_input$digital_d040 <-0
model_input$digital_d041 <-0
model_input$digital_d042 <-0
model_input$digital_d043 <-0
model_input$digital_d044 <-0
model_input$digital_d045 <-0
model_input$digital_d046 <-0
model_input$digital_d047 <-0
model_input$digital_d048 <-0
model_input$digital_d049 <-0
colnames(model_input)[37] <- "data_ss" 
#hardcode，防止在取'data_s'时同时取出'data_sens_prom','data_sale_amt'等
colnames(model_input)[50] <- "data_ww"
colnames(model_input)[6] <- "data_nn"
colnames(model_input) <- tolower(colnames(model_input))

model_equation <- fread(input = bma_output_path, encoding = "UTF-8", data.table = FALSE)
colnames(model_equation) <-
  gsub('bma_output.', "", colnames(model_equation))
model_equation$V1 <- NULL
model_equation$key <- tolower(model_equation$key)
bma_models_final <- fread(
  input = bma_models_final_path, header = TRUE, encoding = "UTF-8", 
  data.table = FALSE, colClasses = "character")
bma_models_final$aic <- gsub(",", '', bma_models_final$aic)
bma_models_final$aic <- as.numeric(bma_models_final$aic)

fix(level)

#generate score
#Extract dimensions -->> dimension为X???_K???_D???_Y???_M???的组合
dimension <- select(model_equation, key)
dimension <- dimension[grep('x0', dimension$key), ] # 如果X维度有将近百余个变量则会出错 注意dimension变成一个vector

#Multiply original exposure by aic
model_input_final_mid <- model_input
rm(model_input)

#model_input_final_mid[, (ncol(model_input_final_mid) + 1):(ncol(model_input_final_mid) + length(dimension))] <-NA
#colnames(model_input_final_mid)[(ncol(model_input_final_mid) - length(dimension) +
#                                     1):(ncol(model_input_final_mid))] <-
#    c(paste0('S_', dimension))
model_input_final_mid[, paste0('S_', dimension)] <- NA # 用于替代上述4行代码。生成S_X???_K???_D???_Y???_M???用于存储X???_K???_D???_Y???_M???的组合之后的曝光量。注意该曝光量可能与实际曝光量不符，需要后期修正。
model_input_final_mid_2 <- model_input_final_mid # 创建model_input_final_mid_2的目的在于：model_input_final_mid_2使用其digital_x/k/d/y/m的原始值与aic相乘计算加权平均，确保model_input_final_mid中digital_x/k/d/y/m不被修改

for (i in 1:nrow(bma_models_final)) {
  match <-
    grep(bma_models_final$key[i],
         colnames(model_input_final_mid_2))
  model_input_final_mid_2[, c(match)] <-
    model_input_final_mid_2[, c(match)] * bma_models_final$aic[i]
} # 对digital_x???/k???/d???/y???/m???加权。权重为对应X/K/D/Y/M的AIC

colnames(model_input_final_mid_2)[grep('digital_', colnames(model_input_final_mid_2))] <-
  toupper(colnames(model_input_final_mid_2)[grep('digital_', colnames(model_input_final_mid_2))]) # 统一为大写便于匹配
col_start <- ncol(model_input_final_mid) - length(dimension) + 1 # col_start为S_X???_K???_D???_Y???_M???中第一列的列序号
remove(i, match)

#Sum up different dimensions (weigthed by AIC), if one of the media exposure is 0, set the sum to 0
for (i in 1:length(grep('S_', colnames(model_input_final_mid_2)))) {
  split <-
    unlist(strsplit(x = colnames(model_input_final_mid_2)[grep('S_', colnames(model_input_final_mid_2))[i]], 
                    split = '_'))
  split <- split[-1] # 获取S_X???_K???_D???_Y???_M???触点对应的X???、K???、D???、Y???，与M???
  split <- toupper(split)
  match_col <-
    match(paste0('DIGITAL_', split),
          colnames(model_input_final_mid_2)) # 根据split获取对应X???、K???、D???、Y???，与M???在model_input_final_mid_2中的列序号
  
  model_input_final_mid_2[, col_start + i - 1] <-
    rowSums(model_input_final_mid_2[, match_col], na.rm = T) / sum(bma_models_final$aic) # 完成对于X???_K???_D???_Y???_M???的加权。
  
  model_input_final_mid_2[which(rowSums(model_input_final_mid_2[, match_col] == 0) > 0), 
                          col_start + i - 1] <- 0 # 一定程度上修正组合X???、K???、D???、Y???、M???时所造成的实际并未发生的曝光量。因为组合过程中并没有考虑该用户是否在某特定X???_K???_D???_Y???_M???上曝光。
  # 在X???_K???_D???_Y???_M???列大于0并不能表明该用户在该触点上曝光。
  # 上述两句的目的在于X???、K???、D???、Y???、M???中，如果任何一个维度水平上为0，则认为该触点上的实际曝光并不存在。
  # （例如：X001上不存在曝光，那么所有涉及X001的X001_K???_D???_Y???_M???组合都不应该存在曝光）
  # 该操作可以修正一部分曝光，但是无法修复所有的曝光。例如如下情形:
  # 某用户在X001_K002与X002_K001上均被曝光，则transformed_adstock上X001与K002均大于0。
  # 基于上述代码的计算得到的X001_K001、X001_K002、X002_K001、X002_K002这些触点皆被曝光，但实际上X001_K001与X002_K002并没有曝光。
  # 且上述代码不会修正这类情形。因此Ada引入'Correct exposure'过程。但是该过程修复的时候没有考虑订单与曝光的先后顺序，所以存在缺陷。
} 
# 至此初步基于non_media_with_seasonality中X???、K???、D???、Y???，与M???的值，使用各x/k/d/y/m的AIC作为权重，加权计算出S_X???_K???_D???_Y???_M???的模型输入值。

#Correct exposure
model_input_final_mid[, col_start:ncol(model_input_final_mid)] <-
  model_input_final_mid_2[, col_start:ncol(model_input_final_mid_2)] # 将在model_input_final_mid_2上计算完毕的S_X???_K???_D???_Y???_M???赋予model_input_final_mid
remove(model_input_final_mid_2, i, match_col, split)
model_input_final_mid$id <- as.numeric(model_input_final_mid$id)
model_input_final_mid$date <-
  as.Date(model_input_final_mid$date)
if (level == 'day') {
  model_input_final_mid <-
    setorder(model_input_final_mid, 
             id, 
             date)#因ret_data_df不含skuid，此处删除skuid
} else if (level == 'second') {
  model_input_final_mid <-
    setorder(model_input_final_mid,
             id,
             date,
             data_sale_ord_tm)#因ret_data_df不含skuid，此处删除skuid
}
colnames(model_input_final_mid)[grep('S_', colnames(model_input_final_mid))] <-
  sub("S_", "", colnames(model_input_final_mid)[grep('S_', colnames(model_input_final_mid))]) 
# 将S_X???_K???_D???_Y???_M???列去除前缀"S_"

#non_media <-
#    as.data.frame(fread('non_media_with_seasonality_blanck.csv'))
non_media <- fread(
  input = "./05_Model/non_media_with_seasonality_blanck.csv", 
  sep = ",", header = TRUE, encoding = "UTF-8", data.table = FALSE)

if (target == TRUE) {
  non_media <-
    filter(non_media, data_target == 1)
} else if (target == FALSE) {
  non_media <- filter(non_media, data_target == 0)
}
if (level == 'day') {
  non_media <- setorder(non_media, id, date)#因ret_data_df不含skuid，此处删除skuid
} else if (level == 'second') {
  non_media <-
    setorder(non_media, id, date, data_sale_ord_tm)#因ret_data_df不含skuid，此处删除skuid
}

if (level == 'day') {
  for (j in 5:ncol(non_media)) { # 注意hardcode。需要确保non_media_with_seasonality_blanck.csv第6列起为X???_K???_D???_Y???_M???
    col <- colnames(non_media)[j]
    match <- match(col, colnames(model_input_final_mid))
    if (!is.na(match)) { # 新增if控制。当样本量很少的时候，会出现non_media中某些X???_K???_D???_Y???_M???无法在model_input_final_mid找到的情况。因为标准代码中修正曝光仅仅考虑是否在X???_K???_D???_Y???_M???曝光，而没有考虑曝光日期与购买日期的先后问题。因此需要使用if控制调整。
      model_input_final_mid[, match] <-
        model_input_final_mid[, match] * non_media[, j]
    } # 新增if控制
  }
} else if (level == 'second') {# 注意hardcode。需要确保non_media_with_seasonality_blanck.csv第7列起为X???_K???_D???_Y???_M???
  for (j in 6:ncol(non_media)) {
    col <- colnames(non_media)[j]
    match <- match(col, colnames(model_input_final_mid))
    if (!is.na(match)) { # 新增if控制。当样本量很少的时候，会出现non_media中某些X???_K???_D???_Y???_M???无法在model_input_final_mid找到的情况。因为标准代码中修正曝光仅仅考虑是否在X???_K???_D???_Y???_M???曝光，而没有考虑曝光日期与购买日期的先后问题。因此需要使用if控制调整。
      model_input_final_mid[, match] <-
        model_input_final_mid[, match] * non_media[, j]
    } # 新增if控制
  }
} # 根据non_media_with_seasonality_blanck.csv执行修正。

#Calculate XBeta1 (weights*exposure)
weights <- select(model_equation, key, weight)
weights$key[grep("data_", weights$key)] <-
  tolower(weights$key[grep("data_", weights$key)])

model_input_final_mid_2 <- model_input_final_mid # 创建model_input_final_mid_2的目的在于使用该对象计算出x*beta+1的值。同时确保model_input_final_mid不变

for (i in 1:(nrow(weights) - 1)) { # 此处为hardcode。最后一行为intercept。需要注意每次从hive取出的行可能会不同。最后一行可能不是intercept。尽管截至目前尚未遇到最后一行不是intercept的情况。
  match <- grep(weights$key[i], colnames(model_input_final_mid_2))
  model_input_final_mid_2[, match] <-
    model_input_final_mid_2[, match] * weights$weight[i]
} # 这一步是将Score与Weight相乘。注意此处不包含intercept，但是包含控制变量

match <-
  match(weights$key[1:nrow(weights) - 1], colnames(model_input_final_mid_2)) # 获取除intercept之外各个变量在model_input_final_mid_2的列序号
model_input_final_mid_2$XBeta_1 <-
  rowSums(model_input_final_mid_2[, match]) + weights$weight[nrow(weights)] # XBeta + 1*Beta0
model_input_final_mid$XBeta_1 <- model_input_final_mid_2$XBeta_1#因ret_data_df不含skuid,原innerjoin代码可能导致一对多，此处直接赋值
remove(model_input_final_mid_2, match, i)
model_input_final_mid$P <-
  1 / (1 + exp(-model_input_final_mid$XBeta_1)) # 执行link function计算。计算出P{Y = 1 | X = x}
model_input_final_mid$square_residual <-
  (1 - model_input_final_mid$P) ^ 2             # 计算residual。

if (level == 'day') {
  model_input_final <-
    select(
      model_input_final_mid,
      id,
      date,#因ret_data_df不含skuid，此处删除skuid
      digital_exposure,
      grep('data_', colnames(model_input_final_mid)),
      grep('x0', colnames(model_input_final_mid)),
      XBeta_1,
      P,
      square_residual
    )
} else if (level == 'second') {
  model_input_final <-
    select(
      model_input_final_mid,
      id,
      date,#因ret_data_df不含skuid，此处删除skuid
      digital_exposure,
      grep('data_', colnames(model_input_final_mid)),
      grep('x0', colnames(model_input_final_mid)),
      XBeta_1,
      P,
      square_residual
    )
}

if (target == TRUE) {
  #write.csv(model_input_final, 'score_target.csv', row.names = F)
  fwrite(x = model_input_final, file = "./05_Model/score_target.csv", 
         append = FALSE, quote = TRUE, sep = ",")
} else if (target == FALSE) {
  #write.csv(model_input_final, 'score_comp.csv', row.names = F)
  fwrite(x = model_input_final, file = "./05_Model/score_comp.csv", 
         append = FALSE, quote = TRUE, sep = ",")
} else {
  #write.csv(model_input_final, 'score.csv', row.names = F)
  fwrite(x = model_input_final, file = "./05_Model/score.csv", 
         append = FALSE, quote = TRUE, sep = ",")
} # 所输出的文件包含的是部分non_media_with_seasonality中字段+X???_K???_D???_Y???_M???的

#generate s1
if (target == TRUE) {
  #model_input_final <- as.data.frame(fread('score_target.csv'))
  model_input_final <- data.table::fread(
    input = "./05_Model/score_target.csv", sep = ",", header = TRUE, 
    encoding = "UTF-8", data.table = FALSE)
} else {
  #model_input_final <- as.data.frame(fread('score.csv'))
  model_input_final <- data.table::fread(
    input = "./05_Model/score.csv", sep = ",", header = TRUE, 
    encoding = "UTF-8", data.table = FALSE)
}
model_input_final <- filter(model_input_final, data_target == 1)
model_input_final$date <- as.Date(model_input_final$date)
model_input_final$data_sale_amt <-
  gsub(",", '', model_input_final$data_sale_amt)
model_input_final$data_sale_amt <-
  as.numeric(model_input_final$data_sale_amt)
weights <- select(model_equation, key, weight)
weights$key[grep("data_", weights$key)] <-
  tolower(weights$key[grep("data_", weights$key)])
intercept <- weights$weight[nrow(weights)]              # bma_output最后一行为intercept
dim_all <- weights$key[-grep('intercept', weights$key)] # dim_all为X???_K???_D???_Y???_M???与控制变量的列名

#Distribute P by media type and calculate sales increase amount
for (i in 1:length(dim_all)) {
  dim <-
    colnames(model_input_final)[grep(dim_all[i], colnames(model_input_final))]
  
  # 创建e_X???_K???_D???_Y???_M???存储：X???_K???_D???_Y???_M???*对应Beta+Beta0。
  # 例如："model_input_final$e_X001_K009_D001_Y001_M001 <- 
  #            intercept + 
  #            model_input_final$X001_K009_D001_Y001_M001 * weights$weight[grep(dim,weights$key)]"
  eval(parse(
    text = paste0(
      'model_input_final$e_',
      dim,
      '<-intercept+model_input_final$',
      dim,
      '*weights$weight[grep(dim,weights$key)]'
    )
  ))
  
  # 创建p_X???_K???_D???_Y???_M???存储：不考虑各控制变量影响的基础上，仅投放该触点广告能带来的P{Y=1|X}概率提升值。
  # 例如："model_input_final$p_X001_K009_D001_Y001_M001 <- 
  #            1 / (1 + exp(-model_input_final$e_X001_K009_D001_Y001_M001)) - 
  #            1 / (1 + exp(-intercept))"
  eval(parse(
    text = paste0(
      'model_input_final$p_',
      dim,
      '<-1/(1+exp(-model_input_final$e_',
      dim,
      '))-1/(1+exp(-intercept))'
    )
  ))
  
  # 创建val_X???_K???_D???_Y???_M???存储：不考虑各控制变量影响的基础上，仅投放该触点广告能带来的incremental sales。
  # 例如："model_input_final$val_X001_K009_D001_Y001_M001 <- 
  #            model_input_final$p_X001_K009_D001_Y001_M001 * model_input_final$data_sale_amt"
  eval(parse(
    text = paste0(
      'model_input_final$val_',
      dim,
      '<-model_input_final$p_',
      dim,
      '*model_input_final$data_sale_amt'
    )
  ))
}

#Calculate sales uplift %
s1 <- model_input_final
rm(model_input_final)
s1$val_intercept <-
  (1 / (1 + exp(-intercept))) * s1$data_sale_amt # 计算intercept对应的incremental sales
s1$sum_val <-
  rowSums(s1[, grep('val_', colnames(s1))], na.rm = T) # 计算所有touchpoint、所有控制变量以及intercept的incremental sales
remove(i)

# 缩放incremental sales的计算
# 例如："s1$val_X001_K009_D001_Y001_M001[which(s1$sum_val!=0)] <- 
#            (s1$val_X001_K009_D001_Y001_M001[which(s1$sum_val!=0)] / s1$sum_val[which(s1$sum_val!=0)]) * 
#            s1$data_sale_amt[which(s1$sum_val!=0)]"
for (i in 1:length(dim_all)) {
  dim <- dim_all[i]
  eval(parse(
    text = paste0(
      's1$val_',
      dim,
      '[which(s1$sum_val!=0)]<-(s1$val_',
      dim,
      '[which(s1$sum_val!=0)]/s1$sum_val[which(s1$sum_val!=0)])*s1$data_sale_amt[which(s1$sum_val!=0)]'
    )
  ))
}
s1$uplift_X <- rowSums(s1[, grep('val_X0', colnames(s1))])
if (level == 'day') {
  s1 <-
    select(
      s1,
      id,
      date,#因ret_data_df不含skuid，此处删除skuid
      digital_exposure,
      grep('data_', colnames(s1)),
      P,
      grep('val_', colnames(s1))
    )
} else if (level == 'second') {
  s1 <-
    select(
      s1,
      id,
      date,#因ret_data_df不含skuid，此处删除skuid
      digital_exposure,
      grep('data_', colnames(s1)),
      data_sale_ord_tm,,
      P,
      grep('val_', colnames(s1))
    )
}
#write.csv(s1, 's1.csv', row.names = F)
fwrite(x = s1, file = "./05_Model/s1.csv", append = FALSE, quote = TRUE, sep = ",")

#Reshape S1
#s1 <- as.data.frame(fread('s1.csv')) # 无需执行
#map <-
#    as.data.frame(fread('./impression_digital_map.csv', encoding = 'UTF-8'))
map <- data.table::fread(
  input = impression_digital_map_path, sep = ",", header = TRUE, 
  encoding = "UTF-8", data.table = FALSE)
map <- select(map, label, id)


s1 <- select(s1, data_sale_amt, grep('val_', colnames(s1)))
s1[nrow(s1) + 1, ] <- colSums(s1[1:ncol(s1)])
s1 <- s1[nrow(s1), ]
s1 <- gather(s1)
s1$key <- toupper(s1$key)
s1$X <- substr(s1$key, 5, 8)
s1$K <- substr(s1$key, 10, 13)
s1$D <- substr(s1$key, 15, 18)
s1$Y <- substr(s1$key, 20, 23)
s1$M <- substr(s1$key, 25, 28)
s1 <- left_join(s1, map, by = c("X" = "id"))
s1 <- left_join(s1, map, by = c("K" = "id"))
s1 <- left_join(s1, map, by = c("D" = "id"))
s1 <- left_join(s1, map, by = c("Y" = "id"))
s1 <- left_join(s1, map, by = c("M" = "id"))
names(s1)[8:12] <-
  c("X_Desc", "K_Desc", "D_Desc", "Y_Desc", "M_Desc")

#write.csv(s1, './result_s1.csv', row.names = F)
data.table::fwrite(
  x = s1, file = "./05_Model/result_s1.csv", append = FALSE, 
  quote = TRUE, sep = ",")