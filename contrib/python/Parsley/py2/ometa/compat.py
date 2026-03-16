import ometa
from ometa.runtime import OMetaGrammarBase
from ometa.grammar import OMeta
from ometa.grammar import loadGrammar
from terml.nodes import termMaker as t

OMeta1 = loadGrammar(ometa, "pymeta_v1",
                     globals(), OMetaGrammarBase)
