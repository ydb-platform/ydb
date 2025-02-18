test
   ![Progress Badge](https://img.shields.io/badge/progress-50%25-brightgreen)
   
![Custom Badge](https://img.shields.io/badge/progress-50%25-brightgreen?style=for-the-badge)

<p>Progress:</p>
<div style="width: 100%; background-color: #f3f3f3;">
  <div style="width: 50%; background-color: #4caf50; text-align: center; color: white;">50%</div>
</div>


<div style="width: 100%; background-color: #de4040; height: 30px; border-radius: 15px; overflow: hidden; position: relative;">
    <div style="width: 70%; height: 100%; background-color: #4caf50; display: flex; justify-content: space-between;">
        <div style="height: 100%; width: 2px; background-color: rgb(20, 20, 20);"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
        <div style="height: 100%; width: 2px; background-color: black;"></div>
    </div>
</div>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        :root {
            --progress-width: 70%; /* Процент заполненности */
            --bar-height: 30px; /* Высота бара */
            --bar-color: #4caf50; /* Цвет заполненной части */
            --bg-color: #e0e0e0; /* Цвет фона бара */
            --divider-width: 2px; /* Ширина разделителя */
            --divider-color: #fff; /* Цвет разделителя */
            --divisions: 10; /* Количество секций */
        }
        
        .progress-container {
            width: 100%;
            background-color: var(--bg-color);
            border-radius: var(--bar-height);
            overflow: hidden;
            position: relative;
            height: var(--bar-height);
        }

        .progress-bar {
            width: var(--progress-width);
            height: 100%;
            background-color: var(--bar-color);
            position: relative;
            display: flex;
        }

        .progress-divider {
            width: var(--divider-width);
            background-color: var(--divider-color);
            height: var(--bar-height);
            margin-left: calc((100% - var(--divisions) * var(--divider-width)) / var(--divisions));
        }
    </style>
</head>
<body>

<div class="progress-container">
    <div class="progress-bar">
        <!-- Генерация разделителей -->
        <template id="divider-template">
            <div class="progress-divider"></div>
        </template>
        <script>
            (function createDividers() {
                const divisions = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--divisions').trim());
                const dividerTemplate = document.getElementById('divider-template');
                const progressBar = document.querySelector('.progress-bar');
                
                for (let i = 1; i < divisions; i++) {
                    const dividerClone = dividerTemplate.content.cloneNode(true);
                    progressBar.appendChild(dividerClone);
                }
            })();
        </script>
    </div>
</div>

</body>
</html>
