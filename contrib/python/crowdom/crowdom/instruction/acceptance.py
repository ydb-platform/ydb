from .. import base, evaluation

acceptance = base.LocalizedString(
    {
        'RU': f"""
<div>
    <input id="hd-1" class="hide" type="checkbox">
    <label for="hd-1"><strong>Нажмите, чтобы изучить информацию о проверке заданий</strong></label>
    <div>
        <p>На проверку отправляются несколько случайно выбранных заданий со страницы. Если доля неверно выполненных
            заданий больше максимальной допустимой, то страница заданий отклоняется, а в сообщение пишутся номера
            неверно выполненных заданий. Также возможен вариант, когда будет оцениваться страница целиком, и она
            будет отклонена, если общее качество страницы ниже допустимого.</p>

        <p>Задания пронумерованы в порядке следования на странице заданий, начиная с 1.</p>

        <div>
            <input id="hd-2" class="hide" type="checkbox">
            <label for="hd-2"><strong>Пример номера задания</strong></label>
            <div>
                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/task-number.jpg"
                     alt="Task number"/>
            </div>
        </div>

        <p>Если вы не согласны с отклонением страницы заданий, подайте апелляцию, написав сообщение в следующем
            формате:</p>

        <ul>
            <li>В первой строке – слово "Апелляция"</li>
            <li>Во второй строке – номер страницы заданий</li>
            <li>В третьей строке – номера неверно выполненных заданий, которые вы считаете выполненными верно</li>
            <li>Начиная с четвертой строки вы можете дать комментарий относительно конкретных заданий или страницы
                заданий в целом
            </li>
        </ul>

        <div>
            <input id="hd-3" class="hide" type="checkbox">
            <label for="hd-3"><strong>Пример при работе из браузера</strong></label>
            <div>
                <p>Проверив раздел "Профиль > История", вы увидите подобную запись:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/ru/rejection-message_browser.png"
                     alt="Rejection message"/>

                <p>Зеленым обведен номер <strong>страницы заданий</strong>, синим – номера неверно выполненных
                    <strong>заданий</strong>.</p>

                <p>Допустим, вы считаете, что вы верно решили задания 9 и 15. Тогда отправьте сообщение в следующем
                    формате:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/ru/appeal-message_browser.png"
                     alt="Appeal message"/>

                <p>Если при написании сообщения есть возможность указать тему, напишите "Апелляция".</p>
            </div>
        </div>
        <div>
            <input id="hd-4" class="hide" type="checkbox">
            <label for="hd-4"><strong>Пример при работе из мобильного приложения</strong></label>
            <div>
                <p>Проверив раздел "Мои задания", вы увидите в списке карточку вида:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/ru/completed-tasks_app.png"
                         alt="Completed tasks"/>

                <p>Нажмите на карточку. На экране задания синим обведены номера неверно выполненных заданий.</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/ru/rejection-message_app.png"
                         alt="Rejection message"/>

                <p>Допустим, вы считаете, что вы верно решили задания 9 и 15. Скопируйте номер страницы заданий, нажав на
                   "Скопировать ID" (обведено зеленым), нажмите на "Написать заказчику" (обведено фиолетовым). Отправьте
                   сообщение в следующем формате:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/ru/appeal-message_app.png"
                     alt="Appeal message"/>
            </div>
        </div>
        <p>В случае, если проводилась проверка страницы в целом, вы увидите сообщение
            "{evaluation.assignment_short_rejection_comment['RU']}". Если вы не согласны с отклонением страницы заданий,
            подайте апелляцию, написав сообщение в описанном выше формате. Указывайте номера тех заданий, которые вы
            считаете выполненными верно.</p>
    </div>
</div>
""",
        'EN': f"""
<div>
    <input id="hd-1" class="hide" type="checkbox">
    <label for="hd-1"><strong>Click to learn the information about solution verification</strong></label>
    <div>
        <p>Several randomly selected tasks from the page are sent for verification. If the percentage of incorrectly
            performed tasks is more than the maximum allowed, then the task page is rejected, and numbers of
            incorrectly performed tasks are written in the message. Alternatively, your assignment will be evaluated
            as a whole and rejected, if general accuracy is below allowed.</p>

        <p>The tasks are numbered in order on the tasks page, starting from 1.</p>

        <div>
            <input id="hd-2" class="hide" type="checkbox">
            <label for="hd-2"><strong>Task number example</strong></label>
            <div>
                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/task-number.jpg"
                     alt="Task number"/>
            </div>
        </div>

        <p>If you do not agree with the rejection of the task page, file an appeal by writing a message in the following
            format:</p>

        <ul>
            <li>In the first line – the word "Appeal"</li>
            <li>In the second line – the number of the task page</li>
            <li>In the third line – the numbers of incorrectly performed tasks that you consider to have been performed
                correctly</li>
            <li>Starting from the fourth line, you can comment on specific tasks or the task page as a whole.</li>
        </ul>

        <div>
            <input id="hd-3" class="hide" type="checkbox">
            <label for="hd-3"><strong>Example when working from the browser</strong></label>
            <div>
                <p>After checking the "Profile > History" section, you will see a similar entry:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/en/rejection-message_browser.png"
                     alt="Rejection message"/>

                <p>Green is the number of the <strong>task page</strong>, blue is the number of incorrectly performed
                    <strong>tasks</strong>.</p>

                <p>Let's say you think that you have correctly performed tasks 6 and 9. Then send a message in the
                    following format:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/en/appeal-message_browser.png"
                     alt="Appeal message"/>

                <p>If it is possible to specify a topic when writing a message, write "Appeal".</p>
            </div>
        </div>
        <div>
            <input id="hd-4" class="hide" type="checkbox">
            <label for="hd-4"><strong>Example when working from a mobile app</strong></label>
            <div>
                <p>After checking the "My Tasks" section, you will see a card like this in the list:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/en/completed-tasks_app.png"
                         alt="Completed tasks"/>

                <p>Click on the card. On the task screen, the numbers of incorrectly performed tasks are outlined in blue.</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/en/rejection-message_app.png"
                         alt="Rejection message"/>

                <p>Let's say you think that you have correctly performed tasks 5 and 9. Copy the task page number by
                    clicking on "Copy ID" (outlined in green), click on "Write to the customer" (outlined in purple).
                    Send the message is in the following format:</p>

                <img src="https://storage.yandexcloud.net/crowdom-public/instructions/en/appeal-message_app.png"
                     alt="Appeal message"/>
            </div>
        </div>
        <p>If your assignment was evaluated as a whole, you will see a message
            "{evaluation.assignment_short_rejection_comment['EN']}". If you do not agree with the page rejection,
            file an appeal by writing a message in the format described above. Specify the numbers of tasks you
            think you have done correctly.</p>
    </div>
</div>
""",
    }
)
