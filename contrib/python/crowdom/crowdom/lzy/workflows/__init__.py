from .classification import (
    Whiteboard as ClassificationWhiteboard,
    create_pool as create_classification_pool,
    add_input_objects,
    run_loop as run_classification_loop,
    get_results as get_classification_results,
)
from .annotation import (
    Whiteboard as AnnotationWhiteboard,
    create_pools as create_annotation_pools,
    run_loop as run_annotation_loop,
    get_results as get_annotation_results,
)
from .sbs import Whiteboard as SbSWhiteboard, Loop as SbSLoop

crowdom_label = 'crowdom'
wb_version = '0.0'  # add it to whiteboard tags in case if data format changed in incompatible way
