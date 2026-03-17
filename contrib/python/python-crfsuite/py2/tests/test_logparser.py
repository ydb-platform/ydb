# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

from pycrfsuite._logparser import TrainLogParser

def _apply_parser(parser, log):
    for line in log:
        event = parser.feed(line)
        if event and event != 'featgen_progress':
            print(parser.last_log, end='')
            print('============== ' + event)


log1 = [
    'Holdout group: 2\n',
    '\n',
    'Feature generation\n',
    'type: CRF1d\n',
    'feature.minfreq: 0.000000\n',
    'feature.possible_states: 0\n',
    'feature.possible_transitions: 1\n',
    '0', '.', '.', '.', '.',
    '1', '.', '.', '.', '.',
    '2', '.', '.', '.', '.',
    '3', '.', '.', '.', '.',
    '4', '.', '.', '.', '.',
    '5', '.', '.', '.', '.',
    '6', '.', '.', '.', '.',
    '7', '.', '.', '.', '.',
    '8', '.', '.', '.', '.',
    '9', '.', '.', '.', '.',
    '10',
    '\n',
    'Number of features: 3948\n',
    'Seconds required: 0.022\n',
    '\n',

    'L-BFGS optimization\n',
    'c1: 1.000000\n',
    'c2: 0.001000\n',
    'num_memories: 6\n',
    'max_iterations: 5\n',
    'epsilon: 0.000010\n',
    'stop: 10\n',
    'delta: 0.000010\n',
    'linesearch: MoreThuente\n',
    'linesearch.max_iterations: 20\n',
    '\n',
    '***** Iteration #1 *****\n',
    'Loss: 1450.519004\n',
    'Feature norm: 1.000000\n',
    'Error norm: 713.784994\n',
    'Active features: 1794\n',
    'Line search trials: 1\n',
    'Line search step: 0.000228\n',
    'Seconds required for this iteration: 0.008\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)\n',
    '    O: (306, 339, 306) (0.9027, 1.0000, 0.9488)\n',
    '    B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)\n',
    '    B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)\n',
    '    I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)\n',
    '    B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)\n',
    '    I-MISC: (0, 0, 0) (******, ******, ******)\n',
    'Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)\n',
    'Item accuracy: 306 / 339 (0.9027)\n',
    'Instance accuracy: 3 / 10 (0.3000)\n',
    '\n',
    '***** Iteration #2 *****\n',
    'Loss: 1363.687719\n',
    'Feature norm: 1.178396\n',
    'Error norm: 370.827506\n',
    'Active features: 1540\n',
    'Line search trials: 1\n',
    'Line search step: 1.000000\n',
    'Seconds required for this iteration: 0.004\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)\n',
    '    O: (306, 339, 306) (0.9027, 1.0000, 0.9488)\n',
    '    B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)\n',
    '    B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)\n',
    '    I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)\n',
    '    B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)\n',
    '    I-MISC: (0, 0, 0) (******, ******, ******)\n',
    'Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)\n',
    'Item accuracy: 306 / 339 (0.9027)\n',
    'Instance accuracy: 3 / 10 (0.3000)\n',
    '\n',
    '***** Iteration #3 *****\n',
    'Loss: 1309.171814\n',
    'Feature norm: 1.266322\n',
    'Error norm: 368.739493\n',
    'Active features: 1308\n',
    'Line search trials: 1\n',
    'Line search step: 1.000000\n',
    'Seconds required for this iteration: 0.003\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)\n',
    '    O: (306, 339, 306) (0.9027, 1.0000, 0.9488)\n',
    '    B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)\n',
    '    B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)\n',
    '    I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)\n',
    '    B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)\n',
    '    I-MISC: (0, 0, 0) (******, ******, ******)\n',
    'Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)\n',
    'Item accuracy: 306 / 339 (0.9027)\n',
    'Instance accuracy: 3 / 10 (0.3000)\n',
    '\n',
    '***** Iteration #4 *****\n',
    'Loss: 1019.561634\n',
    'Feature norm: 1.929814\n',
    'Error norm: 202.976154\n',
    'Active features: 1127\n',
    'Line search trials: 1\n',
    'Line search step: 1.000000\n',
    'Seconds required for this iteration: 0.003\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)\n',
    '    O: (306, 339, 306) (0.9027, 1.0000, 0.9488)\n',
    '    B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)\n',
    '    B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)\n',
    '    I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)\n',
    '    B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)\n',
    '    I-MISC: (0, 0, 0) (******, ******, ******)\n',
    'Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)\n',
    'Item accuracy: 306 / 339 (0.9027)\n',
    'Instance accuracy: 3 / 10 (0.3000)\n',
    '\n',
    '***** Iteration #5 *****\n',
    'Loss: 782.637378\n',
    'Feature norm: 3.539391\n',
    'Error norm: 121.725020\n',
    'Active features: 1035\n',
    'Line search trials: 1\n',
    'Line search step: 1.000000\n',
    'Seconds required for this iteration: 0.003\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (2, 5, 6) (0.4000, 0.3333, 0.3636)\n',
    '    O: (305, 318, 306) (0.9591, 0.9967, 0.9776)\n',
    '    B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)\n',
    '    B-PER: (2, 4, 3) (0.5000, 0.6667, 0.5714)\n',
    '    I-PER: (4, 12, 4) (0.3333, 1.0000, 0.5000)\n',
    '    B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)\n',
    '    I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)\n',
    '    I-MISC: (0, 0, 0) (******, ******, ******)\n',
    'Macro-average precision, recall, F1: (0.243606, 0.332970, 0.268070)\n',
    'Item accuracy: 313 / 339 (0.9233)\n',
    'Instance accuracy: 3 / 10 (0.3000)\n',
    '\n',
    'L-BFGS terminated with the maximum number of iterations\n',
    'Total seconds required for training: 0.022\n',
    '\n',
    'Storing the model\n',
    'Number of active features: 1035 (3948)\n',
    'Number of active attributes: 507 (3350)\n',
    'Number of active labels: 9 (9)\n',
    'Writing labels\n',
    'Writing attributes\n',
    'Writing feature references for transitions\n',
    'Writing feature references for attributes\n',
    'Seconds required: 0.003\n',
    '\n'
]

log2 = [
    'Feature generation\n', # featgen_start
    'type: CRF1d\n',
    'feature.minfreq: 0.000000\n',
    'feature.possible_states: 0\n',
    'feature.possible_transitions: 1\n',
    '0', '.', '.', '.', '.',  # featgen_progress
    '1', '.', '.', '.', '.',
    '2', '.', '.', '.', '.',
    '3', '.', '.', '.', '.',
    '4', '.', '.', '.', '.',
    '5', '.', '.', '.', '.',
    '6', '.', '.', '.', '.',
    '7', '.', '.', '.', '.',
    '8', '.', '.', '.', '.',
    '9', '.', '.', '.', '.',
    '10',
    '\n',
    'Number of features: 4379\n',
    'Seconds required: 0.021\n',  # featgen_end
    '\n',
    'Averaged perceptron\n',
    'max_iterations: 5\n',
    'epsilon: 0.000000\n',
    '\n',
    '***** Iteration #1 *****\n',  # iteration
    'Loss: 16.359638\n',
    'Feature norm: 112.848688\n',
    'Seconds required for this iteration: 0.005\n',  # iteration end
    '\n',
    '***** Iteration #2 *****\n',
    'Loss: 12.449970\n',
    'Feature norm: 126.174821\n',
    'Seconds required for this iteration: 0.004\n',
    '\n',
    '***** Iteration #3 *****\n',
    'Loss: 9.451751\n',
    'Feature norm: 145.482678\n',
    'Seconds required for this iteration: 0.003\n',
    '\n',
    '***** Iteration #4 *****\n',
    'Loss: 8.652287\n',
    'Feature norm: 155.495167\n',
    'Seconds required for this iteration: 0.003\n',
    '\n',
    '***** Iteration #5 *****\n',
    'Loss: 7.442703\n',
    'Feature norm: 166.818487\n',
    'Seconds required for this iteration: 0.002\n',
    '\n',
    'Total seconds required for training: 0.017\n',  # optimization_end
    '\n',
    'Storing the model\n',  # storing_start
    'Number of active features: 2265 (4379)\n',
    'Number of active attributes: 1299 (3350)\n',
    'Number of active labels: 9 (9)\n',
    'Writing labels\n',
    'Writing attributes\n',
    'Writing feature references for transitions\n',
    'Writing feature references for attributes\n',
    'Seconds required: 0.007\n',  # storing_end
    '\n'  # end
]

log3 = [
    'Holdout group: 2\n',
    '\n',
    'Feature generation\n',
    'type: CRF1d\n',
    'feature.minfreq: 0.000000\n',
    'feature.possible_states: 0\n',
    'feature.possible_transitions: 1\n',
    '0', '.', '.', '.', '.',
    '1', '.', '.', '.', '.',
    '2', '.', '.', '.', '.',
    '3', '.', '.', '.', '.',
    '4', '.', '.', '.', '.',
    '5', '.', '.', '.', '.',
    '6', '.', '.', '.', '.',
    '7', '.', '.', '.', '.',
    '8', '.', '.', '.', '.',
    '9', '.', '.', '.', '.',
    '10', '\n',
    'Number of features: 96180\n',
    'Seconds required: 1.263\n',
    '\n',
    'Stochastic Gradient Descent (SGD)\n',
    'c2: 1.000000\n',
    'max_iterations: 5\n',
    'period: 10\n',
    'delta: 0.000001\n',
    '\n',
    'Calibrating the learning rate (eta)\n',
    'calibration.eta: 0.100000\n',
    'calibration.rate: 2.000000\n',
    'calibration.samples: 1000\n',
    'calibration.candidates: 10\n',
    'calibration.max_trials: 20\n',
    'Initial loss: 69781.655352\n',
    'Trial #1 (eta = 0.100000): ',
    '12808.890280\n',
    'Trial #2 (eta = 0.200000): ',
    '26716.801091\n',
    'Trial #3 (eta = 0.400000): ',
    '51219.321368\n',
    'Trial #4 (eta = 0.800000): ',
    '104398.795416 (worse)\n',
    'Trial #5 (eta = 0.050000): ',
    '7804.492475\n',
    'Trial #6 (eta = 0.025000): ',
    '6419.964967\n',
    'Trial #7 (eta = 0.012500): ',
    '6989.552193\n',
    'Trial #8 (eta = 0.006250): ',
    '8303.107921\n',
    'Trial #9 (eta = 0.003125): ',
    '9934.052819\n',
    'Trial #10 (eta = 0.001563): ',
    '11782.234687\n',
    'Trial #11 (eta = 0.000781): ',
    '13777.708878\n',
    'Trial #12 (eta = 0.000391): ',
    '15891.422697\n',
    'Trial #13 (eta = 0.000195): ',
    '18174.499245\n',
    'Trial #14 (eta = 0.000098): ',
    '20955.855446\n',
    'Best learning rate (eta): 0.025000\n',
    'Seconds required: 0.858\n', '\n',
    '***** Epoch #1 *****\n',
    'Loss: 36862.915596\n',
    'Feature L2-norm: 24.717729\n',
    'Learning rate (eta): 0.023810\n',
    'Total number of feature updates: 8323\n',
    'Seconds required for this iteration: 0.462\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (778, 1193, 1084) (0.6521, 0.7177, 0.6834)\n',
    '    O: (45103, 45519, 45355) (0.9909, 0.9944, 0.9926)\n',
    '    B-ORG: (1003, 1326, 1400) (0.7564, 0.7164, 0.7359)\n',
    '    B-PER: (583, 764, 735) (0.7631, 0.7932, 0.7779)\n',
    '    I-PER: (565, 681, 634) (0.8297, 0.8912, 0.8593)\n',
    '    B-MISC: (76, 181, 339) (0.4199, 0.2242, 0.2923)\n',
    '    I-ORG: (735, 933, 1104) (0.7878, 0.6658, 0.7216)\n',
    '    I-LOC: (191, 455, 325) (0.4198, 0.5877, 0.4897)\n',
    '    I-MISC: (204, 481, 557) (0.4241, 0.3662, 0.3931)\n',
    'Macro-average precision, recall, F1: (0.671525, 0.661871, 0.660646)\n',
    'Item accuracy: 49238 / 51533 (0.9555)\n',
    'Instance accuracy: 852 / 1517 (0.5616)\n', '\n',
    '***** Epoch #2 *****\n',
    'Loss: 31176.026308\n',
    'Feature L2-norm: 32.274598\n',
    'Learning rate (eta): 0.022727\n',
    'Total number of feature updates: 16646\n',
    'Seconds required for this iteration: 0.466\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (708, 1018, 1084) (0.6955, 0.6531, 0.6736)\n',
    '    O: (45101, 45611, 45355) (0.9888, 0.9944, 0.9916)\n',
    '    B-ORG: (1053, 1711, 1400) (0.6154, 0.7521, 0.6770)\n',
    '    B-PER: (594, 777, 735) (0.7645, 0.8082, 0.7857)\n',
    '    I-PER: (589, 778, 634) (0.7571, 0.9290, 0.8343)\n',
    '    B-MISC: (94, 264, 339) (0.3561, 0.2773, 0.3118)\n',
    '    I-ORG: (384, 468, 1104) (0.8205, 0.3478, 0.4885)\n',
    '    I-LOC: (166, 285, 325) (0.5825, 0.5108, 0.5443)\n',
    '    I-MISC: (210, 621, 557) (0.3382, 0.3770, 0.3565)\n',
    'Macro-average precision, recall, F1: (0.657608, 0.627752, 0.629257)\n',
    'Item accuracy: 48899 / 51533 (0.9489)\n',
    'Instance accuracy: 813 / 1517 (0.5359)\n', '\n',
    '***** Epoch #3 *****\n',
    'Loss: 23705.719839\n',
    'Feature L2-norm: 35.255014\n',
    'Learning rate (eta): 0.021739\n',
    'Total number of feature updates: 24969\n',
    'Seconds required for this iteration: 0.472\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (808, 1210, 1084) (0.6678, 0.7454, 0.7044)\n',
    '    O: (45244, 45771, 45355) (0.9885, 0.9976, 0.9930)\n',
    '    B-ORG: (1061, 1403, 1400) (0.7562, 0.7579, 0.7570)\n',
    '    B-PER: (588, 728, 735) (0.8077, 0.8000, 0.8038)\n',
    '    I-PER: (565, 640, 634) (0.8828, 0.8912, 0.8870)\n',
    '    B-MISC: (86, 130, 339) (0.6615, 0.2537, 0.3667)\n',
    '    I-ORG: (857, 1148, 1104) (0.7465, 0.7763, 0.7611)\n',
    '    I-LOC: (152, 282, 325) (0.5390, 0.4677, 0.5008)\n',
    '    I-MISC: (170, 221, 557) (0.7692, 0.3052, 0.4370)\n',
    'Macro-average precision, recall, F1: (0.757699, 0.666091, 0.690108)\n',
    'Item accuracy: 49531 / 51533 (0.9612)\n',
    'Instance accuracy: 889 / 1517 (0.5860)\n', '\n',
    '***** Epoch #4 *****\n',
    'Loss: 21273.137466\n',
    'Feature L2-norm: 37.985723\n',
    'Learning rate (eta): 0.020833\n',
    'Total number of feature updates: 33292\n',
    'Seconds required for this iteration: 0.468\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (848, 1276, 1084) (0.6646, 0.7823, 0.7186)\n',
    '    O: (44212, 44389, 45355) (0.9960, 0.9748, 0.9853)\n',
    '    B-ORG: (784, 896, 1400) (0.8750, 0.5600, 0.6829)\n',
    '    B-PER: (582, 686, 735) (0.8484, 0.7918, 0.8191)\n',
    '    I-PER: (570, 647, 634) (0.8810, 0.8991, 0.8899)\n',
    '    B-MISC: (166, 619, 339) (0.2682, 0.4897, 0.3466)\n',
    '    I-ORG: (152, 155, 1104) (0.9806, 0.1377, 0.2415)\n',
    '    I-LOC: (138, 219, 325) (0.6301, 0.4246, 0.5074)\n',
    '    I-MISC: (467, 2646, 557) (0.1765, 0.8384, 0.2916)\n',
    'Macro-average precision, recall, F1: (0.702269, 0.655374, 0.609212)\n',
    'Item accuracy: 47919 / 51533 (0.9299)\n',
    'Instance accuracy: 793 / 1517 (0.5227)\n', '\n',
    '***** Epoch #5 *****\n',
    'Loss: 20806.661564\n',
    'Feature L2-norm: 40.673070\n',
    'Learning rate (eta): 0.020000\n',
    'Total number of feature updates: 41615\n',
    'Seconds required for this iteration: 0.460\n',
    'Performance by label (#match, #model, #ref) (precision, recall, F1):\n',
    '    B-LOC: (689, 892, 1084) (0.7724, 0.6356, 0.6974)\n',
    '    O: (45171, 45556, 45355) (0.9915, 0.9959, 0.9937)\n',
    '    B-ORG: (1214, 1931, 1400) (0.6287, 0.8671, 0.7289)\n',
    '    B-PER: (529, 574, 735) (0.9216, 0.7197, 0.8083)\n',
    '    I-PER: (520, 553, 634) (0.9403, 0.8202, 0.8762)\n',
    '    B-MISC: (77, 96, 339) (0.8021, 0.2271, 0.3540)\n',
    '    I-ORG: (1009, 1678, 1104) (0.6013, 0.9139, 0.7254)\n',
    '    I-LOC: (126, 182, 325) (0.6923, 0.3877, 0.4970)\n',
    '    I-MISC: (57, 71, 557) (0.8028, 0.1023, 0.1815)\n',
    'Macro-average precision, recall, F1: (0.794790, 0.629970, 0.651378)\n',
    'Item accuracy: 49392 / 51533 (0.9585)\n',
    'Instance accuracy: 885 / 1517 (0.5834)\n', '\n',
    'SGD terminated with the maximum number of iterations\n',
    'Loss: 20806.661564\n',
    'Total seconds required for training: 3.350\n', '\n',
    'Storing the model\n',
    'Number of active features: 96180 (96180)\n',
    'Number of active attributes: 76691 (83593)\n',
    'Number of active labels: 9 (9)\n',
    'Writing labels\n',
    'Writing attributes\n',
    'Writing feature references for transitions\n',
    'Writing feature references for attributes\n',
    'Seconds required: 0.329\n', '\n'
]

log4 = [
    'Feature generation\n',
    'type: CRF1d\n',
    'feature.minfreq: 0.000000\n',
    'feature.possible_states: 0\n',
    'feature.possible_transitions: 0\n',
    '0', '.', '.', '.', '.',
    '1', '.', '.', '.', '.',
    '2', '.', '.', '.', '.',
    '3', '.', '.', '.', '.',
    '4', '.', '.', '.', '.',
    '5', '.', '.', '.', '.',
    '6', '.', '.', '.', '.',
    '7', '.', '.', '.', '.',
    '8', '.', '.', '.', '.',
    '9', '.', '.', '.', '.',
    '10', '\n',
    'Number of features: 0\n',
    'Seconds required: 0.001\n', '\n',
    'L-BFGS optimization\n',
    'c1: 0.000000\n', 'c2: 1.000000\n',
    'num_memories: 6\n',
    'max_iterations: 2147483647\n',
    'epsilon: 0.000010\n', 'stop: 10\n',
    'delta: 0.000010\n',
    'linesearch: MoreThuente\n',
    'linesearch.max_iterations: 20\n', '\n',
    'L-BFGS terminated with error code (-1020)\n',
    'Total seconds required for training: 0.000\n', '\n',
    'Storing the model\n',
    'Number of active features: 0 (0)\n',
    'Number of active attributes: 0 (0)\n',
    'Number of active labels: 0 (0)\n',
    'Writing labels\n',
    'Writing attributes\n',
    'Writing feature references for transitions\n',
    'Writing feature references for attributes\n',
    'Seconds required: 0.000\n', '\n'
]

def test_parser_log1():
    """
    >>> parser = TrainLogParser()
    >>> _apply_parser(parser, log1)
    Holdout group: 2
    ============== start
    <BLANKLINE>
    Number of features: 3948
    Seconds required: 0.022
    ============== featgen_end
    <BLANKLINE>
    L-BFGS optimization
    c1: 1.000000
    c2: 0.001000
    num_memories: 6
    max_iterations: 5
    epsilon: 0.000010
    stop: 10
    delta: 0.000010
    linesearch: MoreThuente
    linesearch.max_iterations: 20
    <BLANKLINE>
    ============== prepared
    ***** Iteration #1 *****
    Loss: 1450.519004
    Feature norm: 1.000000
    Error norm: 713.784994
    Active features: 1794
    Line search trials: 1
    Line search step: 0.000228
    Seconds required for this iteration: 0.008
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)
        O: (306, 339, 306) (0.9027, 1.0000, 0.9488)
        B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)
        B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)
        I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)
        B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)
        I-MISC: (0, 0, 0) (******, ******, ******)
    Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)
    Item accuracy: 306 / 339 (0.9027)
    Instance accuracy: 3 / 10 (0.3000)
    <BLANKLINE>
    ============== iteration
    ***** Iteration #2 *****
    Loss: 1363.687719
    Feature norm: 1.178396
    Error norm: 370.827506
    Active features: 1540
    Line search trials: 1
    Line search step: 1.000000
    Seconds required for this iteration: 0.004
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)
        O: (306, 339, 306) (0.9027, 1.0000, 0.9488)
        B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)
        B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)
        I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)
        B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)
        I-MISC: (0, 0, 0) (******, ******, ******)
    Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)
    Item accuracy: 306 / 339 (0.9027)
    Instance accuracy: 3 / 10 (0.3000)
    <BLANKLINE>
    ============== iteration
    ***** Iteration #3 *****
    Loss: 1309.171814
    Feature norm: 1.266322
    Error norm: 368.739493
    Active features: 1308
    Line search trials: 1
    Line search step: 1.000000
    Seconds required for this iteration: 0.003
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)
        O: (306, 339, 306) (0.9027, 1.0000, 0.9488)
        B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)
        B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)
        I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)
        B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)
        I-MISC: (0, 0, 0) (******, ******, ******)
    Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)
    Item accuracy: 306 / 339 (0.9027)
    Instance accuracy: 3 / 10 (0.3000)
    <BLANKLINE>
    ============== iteration
    ***** Iteration #4 *****
    Loss: 1019.561634
    Feature norm: 1.929814
    Error norm: 202.976154
    Active features: 1127
    Line search trials: 1
    Line search step: 1.000000
    Seconds required for this iteration: 0.003
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (0, 0, 6) (0.0000, 0.0000, 0.0000)
        O: (306, 339, 306) (0.9027, 1.0000, 0.9488)
        B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)
        B-PER: (0, 0, 3) (0.0000, 0.0000, 0.0000)
        I-PER: (0, 0, 4) (0.0000, 0.0000, 0.0000)
        B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)
        I-MISC: (0, 0, 0) (******, ******, ******)
    Macro-average precision, recall, F1: (0.100295, 0.111111, 0.105426)
    Item accuracy: 306 / 339 (0.9027)
    Instance accuracy: 3 / 10 (0.3000)
    <BLANKLINE>
    ============== iteration
    ***** Iteration #5 *****
    Loss: 782.637378
    Feature norm: 3.539391
    Error norm: 121.725020
    Active features: 1035
    Line search trials: 1
    Line search step: 1.000000
    Seconds required for this iteration: 0.003
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (2, 5, 6) (0.4000, 0.3333, 0.3636)
        O: (305, 318, 306) (0.9591, 0.9967, 0.9776)
        B-ORG: (0, 0, 9) (0.0000, 0.0000, 0.0000)
        B-PER: (2, 4, 3) (0.5000, 0.6667, 0.5714)
        I-PER: (4, 12, 4) (0.3333, 1.0000, 0.5000)
        B-MISC: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-ORG: (0, 0, 5) (0.0000, 0.0000, 0.0000)
        I-LOC: (0, 0, 1) (0.0000, 0.0000, 0.0000)
        I-MISC: (0, 0, 0) (******, ******, ******)
    Macro-average precision, recall, F1: (0.243606, 0.332970, 0.268070)
    Item accuracy: 313 / 339 (0.9233)
    Instance accuracy: 3 / 10 (0.3000)
    <BLANKLINE>
    ============== iteration
    L-BFGS terminated with the maximum number of iterations
    Total seconds required for training: 0.022
    <BLANKLINE>
    ============== optimization_end
    Storing the model
    Number of active features: 1035 (3948)
    Number of active attributes: 507 (3350)
    Number of active labels: 9 (9)
    Writing labels
    Writing attributes
    Writing feature references for transitions
    Writing feature references for attributes
    Seconds required: 0.003
    <BLANKLINE>
    ============== end

    >>> len(parser.iterations)
    5
    >>> parser.iterations[3]['active_features']
    1127
    """
    pass


def test_parser_log2():
    """
    >>> parser = TrainLogParser()
    >>> _apply_parser(parser, log2)
    Feature generation
    ============== start
    <BLANKLINE>
    Number of features: 4379
    Seconds required: 0.021
    ============== featgen_end
    <BLANKLINE>
    Averaged perceptron
    max_iterations: 5
    epsilon: 0.000000
    <BLANKLINE>
    ============== prepared
    ***** Iteration #1 *****
    Loss: 16.359638
    Feature norm: 112.848688
    Seconds required for this iteration: 0.005
    <BLANKLINE>
    ============== iteration
    ***** Iteration #2 *****
    Loss: 12.449970
    Feature norm: 126.174821
    Seconds required for this iteration: 0.004
    <BLANKLINE>
    ============== iteration
    ***** Iteration #3 *****
    Loss: 9.451751
    Feature norm: 145.482678
    Seconds required for this iteration: 0.003
    <BLANKLINE>
    ============== iteration
    ***** Iteration #4 *****
    Loss: 8.652287
    Feature norm: 155.495167
    Seconds required for this iteration: 0.003
    <BLANKLINE>
    ============== iteration
    ***** Iteration #5 *****
    Loss: 7.442703
    Feature norm: 166.818487
    Seconds required for this iteration: 0.002
    <BLANKLINE>
    ============== iteration
    Total seconds required for training: 0.017
    <BLANKLINE>
    ============== optimization_end
    Storing the model
    Number of active features: 2265 (4379)
    Number of active attributes: 1299 (3350)
    Number of active labels: 9 (9)
    Writing labels
    Writing attributes
    Writing feature references for transitions
    Writing feature references for attributes
    Seconds required: 0.007
    <BLANKLINE>
    ============== end
    """
    pass


def test_parser_log3():
    """
    >>> parser = TrainLogParser()
    >>> _apply_parser(parser, log3)
    Holdout group: 2
    ============== start
    <BLANKLINE>
    Number of features: 96180
    Seconds required: 1.263
    ============== featgen_end
    <BLANKLINE>
    Stochastic Gradient Descent (SGD)
    c2: 1.000000
    max_iterations: 5
    period: 10
    delta: 0.000001
    <BLANKLINE>
    Calibrating the learning rate (eta)
    calibration.eta: 0.100000
    calibration.rate: 2.000000
    calibration.samples: 1000
    calibration.candidates: 10
    calibration.max_trials: 20
    Initial loss: 69781.655352
    Trial #1 (eta = 0.100000): 12808.890280
    Trial #2 (eta = 0.200000): 26716.801091
    Trial #3 (eta = 0.400000): 51219.321368
    Trial #4 (eta = 0.800000): 104398.795416 (worse)
    Trial #5 (eta = 0.050000): 7804.492475
    Trial #6 (eta = 0.025000): 6419.964967
    Trial #7 (eta = 0.012500): 6989.552193
    Trial #8 (eta = 0.006250): 8303.107921
    Trial #9 (eta = 0.003125): 9934.052819
    Trial #10 (eta = 0.001563): 11782.234687
    Trial #11 (eta = 0.000781): 13777.708878
    Trial #12 (eta = 0.000391): 15891.422697
    Trial #13 (eta = 0.000195): 18174.499245
    Trial #14 (eta = 0.000098): 20955.855446
    Best learning rate (eta): 0.025000
    Seconds required: 0.858
    <BLANKLINE>
    ============== prepared
    ***** Epoch #1 *****
    Loss: 36862.915596
    Feature L2-norm: 24.717729
    Learning rate (eta): 0.023810
    Total number of feature updates: 8323
    Seconds required for this iteration: 0.462
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (778, 1193, 1084) (0.6521, 0.7177, 0.6834)
        O: (45103, 45519, 45355) (0.9909, 0.9944, 0.9926)
        B-ORG: (1003, 1326, 1400) (0.7564, 0.7164, 0.7359)
        B-PER: (583, 764, 735) (0.7631, 0.7932, 0.7779)
        I-PER: (565, 681, 634) (0.8297, 0.8912, 0.8593)
        B-MISC: (76, 181, 339) (0.4199, 0.2242, 0.2923)
        I-ORG: (735, 933, 1104) (0.7878, 0.6658, 0.7216)
        I-LOC: (191, 455, 325) (0.4198, 0.5877, 0.4897)
        I-MISC: (204, 481, 557) (0.4241, 0.3662, 0.3931)
    Macro-average precision, recall, F1: (0.671525, 0.661871, 0.660646)
    Item accuracy: 49238 / 51533 (0.9555)
    Instance accuracy: 852 / 1517 (0.5616)
    <BLANKLINE>
    ============== iteration
    ***** Epoch #2 *****
    Loss: 31176.026308
    Feature L2-norm: 32.274598
    Learning rate (eta): 0.022727
    Total number of feature updates: 16646
    Seconds required for this iteration: 0.466
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (708, 1018, 1084) (0.6955, 0.6531, 0.6736)
        O: (45101, 45611, 45355) (0.9888, 0.9944, 0.9916)
        B-ORG: (1053, 1711, 1400) (0.6154, 0.7521, 0.6770)
        B-PER: (594, 777, 735) (0.7645, 0.8082, 0.7857)
        I-PER: (589, 778, 634) (0.7571, 0.9290, 0.8343)
        B-MISC: (94, 264, 339) (0.3561, 0.2773, 0.3118)
        I-ORG: (384, 468, 1104) (0.8205, 0.3478, 0.4885)
        I-LOC: (166, 285, 325) (0.5825, 0.5108, 0.5443)
        I-MISC: (210, 621, 557) (0.3382, 0.3770, 0.3565)
    Macro-average precision, recall, F1: (0.657608, 0.627752, 0.629257)
    Item accuracy: 48899 / 51533 (0.9489)
    Instance accuracy: 813 / 1517 (0.5359)
    <BLANKLINE>
    ============== iteration
    ***** Epoch #3 *****
    Loss: 23705.719839
    Feature L2-norm: 35.255014
    Learning rate (eta): 0.021739
    Total number of feature updates: 24969
    Seconds required for this iteration: 0.472
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (808, 1210, 1084) (0.6678, 0.7454, 0.7044)
        O: (45244, 45771, 45355) (0.9885, 0.9976, 0.9930)
        B-ORG: (1061, 1403, 1400) (0.7562, 0.7579, 0.7570)
        B-PER: (588, 728, 735) (0.8077, 0.8000, 0.8038)
        I-PER: (565, 640, 634) (0.8828, 0.8912, 0.8870)
        B-MISC: (86, 130, 339) (0.6615, 0.2537, 0.3667)
        I-ORG: (857, 1148, 1104) (0.7465, 0.7763, 0.7611)
        I-LOC: (152, 282, 325) (0.5390, 0.4677, 0.5008)
        I-MISC: (170, 221, 557) (0.7692, 0.3052, 0.4370)
    Macro-average precision, recall, F1: (0.757699, 0.666091, 0.690108)
    Item accuracy: 49531 / 51533 (0.9612)
    Instance accuracy: 889 / 1517 (0.5860)
    <BLANKLINE>
    ============== iteration
    ***** Epoch #4 *****
    Loss: 21273.137466
    Feature L2-norm: 37.985723
    Learning rate (eta): 0.020833
    Total number of feature updates: 33292
    Seconds required for this iteration: 0.468
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (848, 1276, 1084) (0.6646, 0.7823, 0.7186)
        O: (44212, 44389, 45355) (0.9960, 0.9748, 0.9853)
        B-ORG: (784, 896, 1400) (0.8750, 0.5600, 0.6829)
        B-PER: (582, 686, 735) (0.8484, 0.7918, 0.8191)
        I-PER: (570, 647, 634) (0.8810, 0.8991, 0.8899)
        B-MISC: (166, 619, 339) (0.2682, 0.4897, 0.3466)
        I-ORG: (152, 155, 1104) (0.9806, 0.1377, 0.2415)
        I-LOC: (138, 219, 325) (0.6301, 0.4246, 0.5074)
        I-MISC: (467, 2646, 557) (0.1765, 0.8384, 0.2916)
    Macro-average precision, recall, F1: (0.702269, 0.655374, 0.609212)
    Item accuracy: 47919 / 51533 (0.9299)
    Instance accuracy: 793 / 1517 (0.5227)
    <BLANKLINE>
    ============== iteration
    ***** Epoch #5 *****
    Loss: 20806.661564
    Feature L2-norm: 40.673070
    Learning rate (eta): 0.020000
    Total number of feature updates: 41615
    Seconds required for this iteration: 0.460
    Performance by label (#match, #model, #ref) (precision, recall, F1):
        B-LOC: (689, 892, 1084) (0.7724, 0.6356, 0.6974)
        O: (45171, 45556, 45355) (0.9915, 0.9959, 0.9937)
        B-ORG: (1214, 1931, 1400) (0.6287, 0.8671, 0.7289)
        B-PER: (529, 574, 735) (0.9216, 0.7197, 0.8083)
        I-PER: (520, 553, 634) (0.9403, 0.8202, 0.8762)
        B-MISC: (77, 96, 339) (0.8021, 0.2271, 0.3540)
        I-ORG: (1009, 1678, 1104) (0.6013, 0.9139, 0.7254)
        I-LOC: (126, 182, 325) (0.6923, 0.3877, 0.4970)
        I-MISC: (57, 71, 557) (0.8028, 0.1023, 0.1815)
    Macro-average precision, recall, F1: (0.794790, 0.629970, 0.651378)
    Item accuracy: 49392 / 51533 (0.9585)
    Instance accuracy: 885 / 1517 (0.5834)
    <BLANKLINE>
    ============== iteration
    SGD terminated with the maximum number of iterations
    Loss: 20806.661564
    Total seconds required for training: 3.350
    <BLANKLINE>
    ============== optimization_end
    Storing the model
    Number of active features: 96180 (96180)
    Number of active attributes: 76691 (83593)
    Number of active labels: 9 (9)
    Writing labels
    Writing attributes
    Writing feature references for transitions
    Writing feature references for attributes
    Seconds required: 0.329
    <BLANKLINE>
    ============== end

    """
    pass


def test_parser_log4():
    """
    >>> parser = TrainLogParser()
    >>> _apply_parser(parser, log4)
    Feature generation
    ============== start
    <BLANKLINE>
    Number of features: 0
    Seconds required: 0.001
    ============== featgen_end
    <BLANKLINE>
    L-BFGS optimization
    c1: 0.000000
    c2: 1.000000
    num_memories: 6
    max_iterations: 2147483647
    epsilon: 0.000010
    stop: 10
    delta: 0.000010
    linesearch: MoreThuente
    linesearch.max_iterations: 20
    <BLANKLINE>
    L-BFGS terminated with error code (-1020)
    ============== prepare_error
    Total seconds required for training: 0.000
    <BLANKLINE>
    ============== optimization_end
    Storing the model
    Number of active features: 0 (0)
    Number of active attributes: 0 (0)
    Number of active labels: 0 (0)
    Writing labels
    Writing attributes
    Writing feature references for transitions
    Writing feature references for attributes
    Seconds required: 0.000
    <BLANKLINE>
    ============== end

    """
    pass
