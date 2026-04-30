def generate_comparison_table(sizes, key_type_name, times, name, times_baseline):
    print(sizes, times, times_baseline)
    html = """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Сравнение алгоритмов</title>
        <style>
            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }
            body {
                font-family: 'Segoe UI', Tahoma, sans-serif;
                padding: 15px;
                background: #f8f9fa;
                display: flex;
                justify-content: center;
                align-items: center;
            }
            .comparison-card {
                background: white;
                border-radius: 10px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.08);
                padding: 25px;
                max-width: 700px;
                width: 100%;
            }
            h1 {
                text-align: center;
                color: #202124;
                margin-bottom: 20px;
                font-size: 22px;
                font-weight: 600;
            }
            .compact-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 14px;
            }
            .compact-table th {
                background: #4285F4;
                color: white;
                font-weight: 500;
                padding: 12px 8px;
                text-align: center;
            }
            .compact-table td {
                padding: 10px 8px;
                text-align: center;
                border-bottom: 1px solid #eaebee;
            }
            .compact-table tr:last-child td {
                border-bottom: none;
            }
            .compact-table tr:nth-child(even) {
                background-color: #fafbfc;
            }
            .compact-table tr:hover {
                background-color: #f1f8ff;
            }
            .ratio-cell {
                font-weight: 700;
                font-size: 15px;
            }
            .a-faster {
                color: #0f9d58;
                background-color: #e6f4ea;
            }
            .b-faster {
                color: #db4437;
                background-color: #fce8e6;
            }
            .same {
                color: #fbbc04;
                background-color: #fef7e0;
            }
            .size-cell {
                font-weight: 600;
                color: #202124;
            }
            .time-cell {
                font-family: 'Courier New', monospace;
                color: #5f6368;
                font-size: 13px;
            }
            .legend {
                display: flex;
                justify-content: center;
                gap: 20px;
                margin-top: 20px;
                font-size: 13px;
            }
            .legend-item {
                display: flex;
                align-items: center;
                padding: 5px 10px;
                border-radius: 4px;
            }
            .legend-a-faster {
                background-color: #e6f4ea;
                color: #0f9d58;
            }
            .legend-b-faster {
                background-color: #fce8e6;
                color: #db4437;
            }
            .legend-dot {
                width: 12px;
                height: 12px;
                border-radius: 50%;
                margin-right: 6px;
            }
            .a-dot {
                background-color: #0f9d58;
            }
            .b-dot {
                background-color: #db4437;
            }
        </style>
    </head>
    <body>
        <div class="comparison-card">"""+f"<h1> {name} vs GraceJoin, join key: {key_type_name} </h1>"+"""
            <table class="compact-table">
                <tr>
                    <th>Размер левой таблички</th>
                    <th>Размер правой таблички</th>\n
    """ + f"<th>{name} (мс)</th>\n" + """
                    <th>GraceJoin (мс)</th>
                    <th>Ускорение (раз)</th>
                </tr>
    """
    
    for i in range(len(sizes)):
        n = sizes[i]
        a = times[i]
        b = times_baseline[i]
        
        # Рассчитываем отношение производительности (A/B)
        if b == 0:
            # Если B = 0, а A > 0, то A бесконечно медленнее
            ratio = float('inf') if a > 0 else 1.0
        elif a == 0:
            # Если A = 0, а B > 0, то A бесконечно быстрее
            ratio = float('inf')
        else:
            ratio = b / a  # Во сколько раз A быстрее B
        
        # Определяем стиль ячейки
        if ratio > 1.0:
            ratio_class = "a-faster"
            ratio_str = f"{ratio:.3f}"
        elif ratio < 1.0:
            ratio_class = "b-faster"
            # Показываем обратное отношение для лучшего восприятия
            ratio_str = f"{ratio:.3f}" if ratio > 0 else "∞"
        elif ratio == float('inf'):
            ratio_class = "a-faster"
            ratio_str = "∞"
        else:
            ratio_class = "same"
            ratio_str = "1.000"
        
        html += f"""
            <tr>
                <td class="size-cell">{n}</td>
                <td class="size-cell">{int(n / 128)}</td>
                <td class="time-cell">{a:.6f}</td>
                <td class="time-cell">{b:.6f}</td>
                <td class="ratio-cell {ratio_class}">{ratio_str}</td>
            </tr>
        """
    
    html += """
            </table>
            <div class="legend">
                <div class="legend-item legend-a-faster">
                    <span class="legend-dot a-dot"></span>
            """ + f"<span>{name} быстрее GraceJoin</span>" + """
                </div>
                <div class="legend-item legend-b-faster">
                    <span class="legend-dot b-dot"></span>
            """ + f"<span>GraceJoin быстрее {name}</span>" + """
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    return html
