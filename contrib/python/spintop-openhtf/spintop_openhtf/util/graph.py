import random

def auto_graph_to_plotly_fig(graph, seed=98):
    import random
    random.seed(seed) # Always the same seed to generate the same graph everytime
    
    coord_data = {}
    coords = {}
    
    def coord_fn(data):
        qualname = data['qualname']
        if qualname not in coords:
            depth = len(qualname.split('.'))
            x = coord_data.get(depth, 0)
            y = depth + random.uniform(0, 0.50)*(x%2)
            
            x = x - random.uniform(0, 0.50)*((y-1)%2)
            
            coord_data[depth] = coord_data.get(depth, 0) + 1
            coords[qualname] = (x, y)
        return coords[qualname]
    
    return graph_to_plotly_fig(graph, coord_fn)

def graph_to_plotly_fig(graph, coord_fn, text_fn=None):
    import plotly.graph_objects as go
    
    
    annotations = []
    
    edge_x = []
    edge_y = []
    edge_text = []
    
    for edge in graph.edges():
        
        x0, y0 = coord_fn(graph.node[edge[0]])
        x1, y1 = coord_fn(graph.node[edge[1]])
        annotations.append(go.layout.Annotation(
            x=x1,
            y=y1,
            showarrow=True,
            arrowhead=2,
            arrowsize=4,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            ax=x0,
            ay=y0,
        ))
        
    
    # edge_trace = go.Scatter(
    #     x=edge_x, y=edge_y,
    #     line=dict(width=0.5, color='#888'),
    #     hoverinfo='none',
    #     mode='lines')

    node_x = []
    node_y = []
    colors = []
    # node_text = []
    for nodename in graph.nodes():
        node = graph.node[nodename]
        x, y = coord_fn(node)
        node_x.append(x)
        node_y.append(y)
        if text_fn:
            text = text_fn(node)
        else:
            text = nodename
        
        if node['node'].testable:
            color = 'green'
        else:
            color = 'grey'
        colors.append(color)
        # node_text.append(text)
        annotations.append(go.layout.Annotation(
            x=x,
            y=y,
            xref="x",
            yref="y",
            text=text,
            ax=0,
            ay=-40
        ))

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            # colorscale options
            #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
            #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
            #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
            colorscale='YlGnBu',
            reversescale=True,
            color=colors,
            size=10,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))
    
    # node_trace.text = node_text
    
    fig = go.Figure(data=[node_trace],
             layout=go.Layout(
                title='Network graph made with Python',
                titlefont_size=16,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=annotations,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                )

    return fig