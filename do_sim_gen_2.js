
function do_sim_gen(i_name, num_sims){

    //debugger;
    var NUM_DICES = 5;
    var all_scores = Array(num_sims).fill(-1);
    var cat_scores;
    var cat_names = ['ones', 'twos', 'threes', 'fours', 'fives', 'sixes', 'threeOfAKind', 'fourOfAKind', 'fullHouse', 'smallStraightu', 'largeStraightu', 'chance', 'yatzy'];
    var dices = Array(NUM_DICES).fill(-1);
    var dice_lock;
    var final_score = 0;
  
    const num_features = 47
    const num_cat = cat_names.length
  
    var X = Array.from({ length: num_sims*num_cat }, () => 
      Array.from({ length: num_features+1 }, () => 0)
    );
  
    for (var i_sim = 0; i_sim < num_sims; i_sim++) { 
  
      final_score = 0;
      cat_scores = Array(13).fill(-1);
  
      for (var i_cat = 0; i_cat < cat_scores.length; i_cat++) { 
  
        //console.log("i_cat", i_cat, cat_scores.length);
        dice_lock = []
        score_sum = 0;
        for (var rolls_left = 2; rolls_left >= 0; rolls_left--) { 
          var type = "ukus";
          var myScoreCalc = {};
          var best_dice_sndr = []; 
          if (!myScoreCalc[type]) {
                myScoreCalc[type] = new (SWYatzyScoreCalc)(type);
            }
          dices = [];
          for (var i_dice = 0; i_dice < NUM_DICES-dice_lock.length; i_dice++) { 
              dices.push( Math.floor(Math.random() * 6) + 1 );
          }
          dices = dices.concat(dice_lock);
  
          //console.log("Alejandro123 ",dices);
  
          var feature_vector_first_half = Array(1).fill(-1);
          feature_vector_first_half = feature_vector_first_half.concat( dices );
          feature_vector_first_half = feature_vector_first_half.concat( cat_scores );
  
          //console.log("Alejandro_feature_scores", cat_scores );
          //console.log("Alejandro_feature_0", computando_el_scoro(cat_scores) );
  
          feature_vector_first_half = feature_vector_first_half.concat( [computando_el_scoro(cat_scores),300] );
          
          //console.log("Alejandro_feature_1", feature_vector_first_half);
  
          var feature_vector = add_features(feature_vector_first_half);
          feature_vector = feature_vector.slice(1);
          feature_vector.splice(19, 1);
          //console.log("Alejandro_feature_2", feature_vector);
  
          var scores = [];
  
          var available_cats = [];
          for (var i = 0; i < cat_scores.length; i++) { 
            if (cat_scores[i]<0){
              available_cats.push( cat_names[i] )
            }
          }
          
          data =  {
            availableCategories : available_cats,
          };
          //scores =  make_prediction2(feature_vector, model_roll_1, model_roll_2, model_final_roll, rolls_left);
          //if (rolls_left >= 1){
          //  scores[11] = 0.001
          //}
  
          if (rolls_left==0){
            per_lab_andr = myScoreCalc[type].getBestCatChoice(data.availableCategories, dices, score_sum);
            predicted_label = per_lab_andr[0];
            dice_lock = [];
            //console.log("Alejandro123 ",feature_vector);
            //console.log("Alejandro123 ",cat_names.indexOf(predicted_label));
            var x_fv = feature_vector;
            x_fv.unshift( cat_names.indexOf(predicted_label) );
            X[i_sim*num_cat + i_cat] =  feature_vector;
            //console.log("Alejandro123_predicted_label ",predicted_label);
            //console.log("Alejandro123_predicted_label_ind ", cat_names.indexOf(predicted_label) );
            //console.log("Alejandro123_predicted_label_ind_fv ", feature_vector );
          };
  
          if (rolls_left>=1){
            per_lab_andr = myScoreCalc[type].getBestCatChoice(data.availableCategories, dices, score_sum);
            predicted_label = per_lab_andr[0];
            best_dice_sndr = myScoreCalc[type].getBestDiceChoice(data.availableCategories, dices, rolls_left, score_sum);
            dice_lock = best_dice_sndr[0];
            //console.log("Alejandro123_dice_lock ",dice_lock);
          };
          
  
          // // ELENA CODE
          // var type = "ukus";
          // var myScoreCalc = {};
          // var best_dice_sndr = []; 
          // if (!myScoreCalc[type]) {
          //     myScoreCalc[type] = new (SWYatzyScoreCalc)(type);
          // }
          // per_lab_andr = myScoreCalc[type].getBestCatChoice(data.availableCategories, dices, score_sum);
          // if (rolls_left > 0) {
          // best_dice_sndr = myScoreCalc[type].getBestDiceChoice(data.availableCategories, dices, rolls_left, score_sum);}
          // predicted_label = per_lab_andr[0];
          // dice_lock = best_dice_sndr[0];
          // console.log("Simulation andreas picked label: ", predicted_label);
          // console.log("Simulation andreas picked dices: ", dice_lock);
  
          
          
          // decide_dices_variable = decide_dices(dices, predicted_label);
          // dice_lock = decide_dices_variable["dice_lock"]
  
          // console.log("Alejandro", feature_vector);
          // console.log("Alejandro", scores);
          // console.log("Alejandro Dices", dices);
          // console.log("Alejandro", predicted_label);
          //console.log("Alejandro", available_cats);
          //console.log("Alejandro dice_lock", dice_lock);
          // if (rolls_left==0){
          //   console.log("Alejandro ----------------------------------------");
          // }
        }
        ind_cat = cat_names.indexOf(predicted_label)
        cat_scores[ ind_cat ] = feature_vector[ ind_cat + 33 ]
        score_sum = score_sum + feature_vector[ ind_cat + 33 ]
        //console.log("Alejandro", cat_scores);
      }
  
      final_score = computando_el_scoro(cat_scores);
      //console.log("Alejandro final_score", final_score);
      all_scores[i_sim] = final_score;
    };
    //console.log("All scores from simulation: ", all_scores);
    //console.log("All scores from simulation string: ", JSON.stringify(all_scores));
  
  
    var csv_string = X.map(function(d){
      return JSON.stringify(d);
    })
    .join('\n') 
    .replace(/(^\[)|(\]$)/mg, '');
  
    //console.log("_X ", csv_string);
  
    folder_name = "data2/"
    console.log("putssmuts",putss( folder_name.concat( i_name, ".csv" ), csv_string));
  
    // const fs = __non_webpack_require__("fs")
    // fs.writeFile("temp.txt", "ffff")
  
    // const params = {
    //   Bucket: BUCKET_NAME,
    //   Key: 'file.csv', // File name you want to save as in S3
    //   Body: '1,2,3'
    // };
    
    // // Uploading files to the bucket
    // s3.upload(params, function(err, data) {
    //   if (err) {
    //       throw err;
    //   }
    //   console.log(`File uploaded successfully. ${data.Location}`);
    // });
  
    //S3Object stringObject = new S3Object("HelloWorld.txt", "Hello World!");
    //s3.putObject(BUCKET_NAME, stringObject);
  
    return all_scores;
  };
  
  