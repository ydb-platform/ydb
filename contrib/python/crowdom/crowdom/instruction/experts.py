from .. import base, experts, project

correct, wrong = base.BinaryEvaluation.get_display_labels()

task_labeling = project.Scenario.EXPERT_LABELING_OF_TASKS.project_name_suffix
solution_labeling = project.Scenario.EXPERT_LABELING_OF_SOLVED_TASKS.project_name_suffix

task_verification = experts.ExpertCase.TASK_VERIFICATION.label
labeling_quality_verification = experts.ExpertCase.LABELING_QUALITY_VERIFICATION.label

image_classification_task = base.LocalizedString(
    {
        'EN': 'classification of animals in photos',
        'RU': 'классификация животных на фотографиях',
    }
)
audio_transcript_task = base.LocalizedString(
    {
        'EN': 'transcript of speech from audio',
        'RU': 'расшифровка речи из аудиозаписей',
    }
)

expert_evaluation = base.LocalizedString(
    {
        'RU': """
<li>Бинарная оценка решения <code>EVAL</code>: <code>{CORRECT}</code>/<code>{WRONG}</code>. Этот пункт всегда должен
отражать, правильно ли в соответствии с инструкцией по аннотации выполнено исходное задание.</li>
    """.format(
            WRONG=wrong['RU'], CORRECT=correct['RU']
        ),
        'EN': """
<li>Binary evaluation of solution <code>EVAL</code>: <code>{CORRECT}</code>/<code>{WRONG}</code>. This option should always
reflect whether the annotation was done accurately according to the instruction or not.</li>
    """.format(
            WRONG=wrong['EN'], CORRECT=correct['EN']
        ),
    }
)

expert_solve_task = base.LocalizedString(
    {
        'RU': '<li>Решить задание, как это сделал бы исполнитель согласно инструкции.</li>',
        'EN': '<li>Complete the task as the worker would do according to the instructions.</li>',
    }
)

expert_solve_annotation_task = base.LocalizedString(
    {
        'RU': """
<li>Решить задание, либо правильно – как это сделал бы исполнитель согласно инструкции, либо специально неправильно –
допустите ошибку, нарушив один из важных пунктов инструкции.</li>
<li>
    В поле <code>EVAL</code> соответственно выберите, правильно выполнили задание или нет.
</li>
""",
        'EN': """
<li>Complete the task either accurately – as the worker would do according to the instructions, or make some errors on
purpose, violating one of the important points of the instructions.</li>
<li>
    In <code>EVAL</code> field choose whether you completed the task accurately or not.
</li>
""",
    }
)

expert_extra_comment_reason = base.LocalizedString(
    {
        'RU': 'допустили при выполнении задания специальную ошибку или',
        'EN': 'made some errors on purpose while completing the task or',
    }
)

expert_fill_eval = base.LocalizedString(
    {
        'RU': """В поле <code>EVAL</code> выберите, верно выполнено задание или нет.""",
        'EN': """In <code>EVAL</code> field choose, whether you've completed the task accurately or not.""",
    }
)

experts = base.LocalizedString(
    {
        'RU': """
<div>
    <h1>Цель задания</h1>

    <p>
        Ваша цель – быть посредником между заказчиком и исполнителями, предоставляя двусторонние гарантии:
    </p>

    <ul>
        <li>Для исполнителей – гарантия предоставления четко поставленной задачи.</li>
        <li>Для заказчика – гарантия предоставления качественных решений.</li>
    </ul>

    <p><strong>Объект</strong> – проверяемое вами задание или задание с решением (далее просто <em>решение</em>).</p>

    <p><strong>Источник объекта</strong> – заказчик или исполнитель.</p>

    <p>
        Объекты, их источники, а также необходимые действия, определяются <strong>сценарием</strong>, указанным в
        карточке выполняемого вами задания.
    </p>

    <h1>Разметка объектов</h1>

    <p>
        Интерфейс предлагаемых вам заданий всегда построен одинаковым образом: сначала следует интерфейс
        <em>объекта</em>, затем фиксированная часть с его экспертной разметкой, отделяемая горизонтальной чертой.
    </p>

    <p>
        Интерфейс <em>объекта</em> зависит от решаемой заказчиком задачи. Например, если он решает задачу
        <em>{TASK_NAME}</em>, то интерфейс целиком может выглядеть так:
    </p>

    <div>
        <input id="hd-30" class="hide" type="checkbox">
        <label for="hd-30"><strong>Пример <em>объекта</em>-задания</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-tasks_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-tasks_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <div>
        <input id="hd-31" class="hide" type="checkbox">
        <label for="hd-31"><strong>Пример <em>объекта</em>-решения</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-solutions_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-solutions_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>
        Разметка <em>объекта</em> включает в себя {EXPERT_LABELING_OBJECT_COUNT} пункта:
    </p>

    <ul>
        {EXPERT_EVALUATION}
        <li>Бинарная оценка <code>OK</code>: <em>{CORRECT}</em>/<em>{WRONG}</em>.</li>
        <li>Текстовый комментарий <code>COMMENT</code>.</li>
    </ul>

    <p>Способ заполнения пунктов <code>OK</code> и <code>COMMENT</code> зависит от <em>сценария</em>.</p>

    <h1>Сценарии</h1>

    <h2>Проверка заданий заказчика</h2>

    <p>
        В ленте заданий Толоки вы увидите карточку с суффиксом <em>({TASK_LABELING})</em> в названии и примечанием <em>{TASK_VERIFICATION}</em>:
    </p>

    <div>
        <input id="hd-32" class="hide" type="checkbox">
        <label for="hd-32"><strong>Пример карточки задания</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-tasks_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-tasks_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>
        Не все заказчики хорошо понимают, как лучше составлять инструкцию и интерфейс заданий так, чтобы исполнители
        четко понимали поставленное задание и не испытывали трудностей при решении.
    </p>

    <p>
        Даже при наличии хорошо составленной инструкции, размечаемые данные могут не соответствовать написанному в
        инструкции.
    </p>

    <p>
        Перечислим критерии хорошо сформулированного задания:
    </p>

    <ul>
        <li><u>Полнота</u> – в инструкции перечислены все случаи, наблюдаемые в потоке размечаемых данных.</li>
        <li><u>Понятность</u> – инструкция написана простым и понятным языком.</li>
        <li><u>Непротиворечивость</u> – в инструкции нет пунктов, которые противоречат друг другу.</li>
        <li><u>Краткость</u> – инструкция содержит минимум информации, нужный для объяснения сути задания.</li>
        <li><u>Удобство</u> – интерфейс заданий удобен и предполагает минимум действий для решения задания.</li>
    </ul>

    <p>
        Вам будут показаны задания заказчика. Цель данного сценария:
    </p>

    <ul>
        <li>
            Дать обратную связь заказчику при необходимости доработать задания. Обратная связь бывает:
            <ul>
                <li><u>По-<em>объектная</em></u>, которую вы укажете при разметке объекта.</li>
                <li><u>Общая</u>, которую вы можете сообщить в чате с заказчиком.</li>
            </ul>
        </li>
        <li>
            Сформировать набор обучающих заданий, который должен соответствовать следующим критериям:
            <ul>
                <li>
                    <u>Полнота</u> – покрытие всех случаев из инструкции, при условии наличия соответствующих
                    примеров.
                </li>
                <li>
                    <u>Содержательность</u> – примерно одинаковое количество информации для каждого случая. Чем
                    сложнее случай, тем больше обучающих примеров может потребоваться для необходимого количества
                    информации.
                </li>
            </ul>
        </li>
    </ul>

    <p>
        Для каждого задания вам требуется:
    </p>

    <ul>
        {EXPERT_SOLVE_TASK}
        <li>
            Если задание <strong>некорректное</strong> – соответствующий случай не описан в инструкции, оно вообще
            не соответствует инструкции, возникли технические сложности вроде ошибки 404, либо по какой-то иной
            причине, выберите <code>{WRONG}</code> в поле <code>OK</code> и укажите <code>COMMENT</code> с описанием
            проблемы.
        </li>
        <li>
            Если вышеперечисленных проблем нет, то задание <strong>корректное</strong>, выберите
            <code>{CORRECT}</code> в поле <code>OK</code>. Если вы {EXTRA_COMMENT_REASON} считаете, что задание должно попасть в обучение, укажите <code>COMMENT</code>,
            исполнители с ним смогут ознакомиться при прохождении обучения. В противном случае, оставьте
            <code>COMMENT</code> пустым.
        </li>
    </ul>

    <h2>Проверка решенных заданий заказчика</h2>

    <p>
        В ленте заданий Толоки вы увидите карточку с суффиксом <em>{EXTRA_SUFFIX}({SOLUTION_LABELING})</em> в названии и примечанием <em>{TASK_VERIFICATION}</em>:
    </p>
    <div>
        <input id="hd-33" class="hide" type="checkbox">
        <label for="hd-33"><strong>Пример карточки задания</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-solutions_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-solutions_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>
        Сценарий эквивалентен предыдущему, с тем лишь отличием, что заказчик предоставляет задания с решениями. {EXPERT_FILL_EVAL}
    </p>
    <p>
        Правила разметки <em>объектов</em> (решений) немного изменятся. Решение <em>некорректное</em>, если:
    </p>

    <ul>
        <li>Некорректно задание (см. выше критерии и соответствующие действия)</li>
        <li>
            Некорректно решение – оно не соответствует инструкции, выберите <code>{WRONG}</code> в поле <code>OK</code> и укажите
            причину в <code>COMMENT</code>.
        </li>
    </ul>

    <h2>Проверка качества разметки</h2>

    <p>
        В ленте заданий Толоки вы увидите карточку с суффиксом <em>({SOLUTION_LABELING})</em> в названии и примечанием <em>{LABELING_QUALITY_VERIFICATION}</em>:
    </p>

    <div>
        <input id="hd-34" class="hide" type="checkbox">
        <label for="hd-34"><strong>Пример карточки задания</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_verification_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="verification-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_verification_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="verification-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>Сценарий эквивалентен предыдущему. Источник решений в данном сценарии – разметка исполнителей. Оценивать нужно только правильность решения. </p>

    <p>
        Решение <em>некорректное</em>, если оно не соответствует инструкции, выберите <code>{WRONG}</code> в поле
        <code>OK</code> и опционально укажите причину в <code>COMMENT</code>.
    </p>
</div>
""",
        'EN': """
<div>
    <h1>Task purpose</h1>

    <p>
        Your goal is to be an intermediary between the customer and the workers, providing bilateral guarantees:
    </p>

    <ul>
        <li>For performers, it is a guarantee of providing a clearly defined task.</li>
        <li>For the customer – a guarantee of providing quality solutions.</li>
    </ul>

    <p><strong>Object</strong> – the task or the task with the solution (next, just a <em>solution</em>) you are checking.</p>

    <p><strong>Object source</strong> – customer or worker.</p>

    <p>
        The objects, their sources, as well as the necessary actions are determined by the <strong>scenario</strong>
        specified in the card of the task you are performing.
    </p>

    <h1>Objects labeling</h1>

    <p>
        The interface of the tasks offered to you is always built in the same way: the interface follows first of the
        <em>object</em>, then the fixed part with its expert labeling, separated by a horizontal line.
    </p>

    <p>
        The interface of the <em>object</em> depends on the task being solved by the customer. For example, if he
        solves a problem <em>{TASK_NAME}</em>, then the entire interface may look like this:
    </p>

    <div>
        <input id="hd-30" class="hide" type="checkbox">
        <label for="hd-30"><strong>Task <em>object</em> example</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-tasks_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-tasks_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <div>
        <input id="hd-31" class="hide" type="checkbox">
        <label for="hd-31"><strong>Solution <em>object</em> example</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-solutions_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-ui_expert-labeling-of-solutions_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-object" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>
        <em>Object</em> labeling includes {EXPERT_LABELING_OBJECT_COUNT} items:
    </p>

    <ul>
        {EXPERT_EVALUATION}
        <li>Binary evaluation <code>OK</code>: <em>{CORRECT}</em>/<em>{WRONG}</em>.</li>
        <li>Textual comment <code>COMMENT</code>.</li>
    </ul>

    <p>The way to fill in these items depends on the <em>scenario</em>.</p>

    <h1>Scenarios</h1>

    <h2>Checking the customer's tasks</h2>

    <p>
        In the Toloka task feed, you will see a card with the suffix <em>({TASK_LABELING})</em> in the title and the note <em>{TASK_VERIFICATION}</em>:
    </p>

    <div>
        <input id="hd-32" class="hide" type="checkbox">
        <label for="hd-32"><strong>Task card example</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-tasks_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-tasks_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>
        Not all customers understand well how best to compile instructions and the interface of tasks so that workers
        clearly understand the task and had no difficulties in performing it.
    </p>

    <p>
        Let's list the criteria for a well-formulated task:
    </p>

    <ul>
        <li><u>Completeness</u> - the instructions list all the cases observed in the data stream.</li>
        <li><u>Clarity</u> - the instruction is written in a simple and understandable language.</li>
        <li><u>Consistency</u> – there are no points in the instructions that contradict each other.</li>
        <li><u>Brevity</u> - the instruction contains a minimum of information necessary to explain the essence of the
            task.</li>
        <li><u>Convenience</u> - the task interface is convenient and involves a minimum of actions to perform the
            task.</li>
    </ul>

    <p>
        You will be shown the customer's tasks. The purpose of this scenario:
    </p>

    <ul>
        <li>
            Give feedback to the customer, if necessary, to rework the tasks. Types of feedback are:
            <ul>
                <li><u>By-<em>object</em></u>, which you specify when labeling the object.</li>
                <li><u>General</u>, which you can inform in a chat with the customer.</li>
            </ul>
        </li>
        <li>
            Create a set of training tasks that must meet the following criteria:
            <ul>
                <li>
                    <u>Completeness</u> - coverage of all cases from the instructions, subject to the availability of
                    appropriate examples.
                </li>
                <li>
                    <u>Informativeness</u> – approximately the same amount of information for each case. The more
                    complicated the case, the more training examples may be required for the required amount of
                    information.
                </li>
            </ul>
        </li>
    </ul>

    <p>
        For each task you need:
    </p>

    <ul>
        {EXPERT_SOLVE_TASK}
        <li>Perform the task as the worker would do according to the instructions.</li>
        <li>
            If the task is <strong>incorrect</strong> - the corresponding case is not described in the instructions, it
            is generally does not comply with the instructions, there were technical difficulties like error 404, or
            for some other reason, choose <code>{WRONG}</code> in <code>OK</code> field and specify
            <code>COMMENT</code> with a problems description.
        </li>
        <li>
            If there are no problems listed above, then the task is <strong>correct</strong>, choose
            <code>{CORRECT}</code> in <code>OK</code> field. If you {EXTRA_COMMENT_REASON} think that the task should be
            included in the training, specify <code>COMMENT</code>, which workers will be able to learn during
            training. Otherwise, leave <code>COMMENT</code> empty.
        </li>
    </ul>

    <h2>Checking the customer's solved tasks</h2>

    <p>
        In the Toloka task feed, you will see a card with the suffix <em>{EXTRA_SUFFIX}({SOLUTION_LABELING})</em> in the title and the note <em>{TASK_VERIFICATION}</em>:
    </p>

    <div>
        <input id="hd-33" class="hide" type="checkbox">
        <label for="hd-33"><strong>Task card example</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-solutions_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_test-launch-on-solutions_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>The scenario is equivalent to the previous one, with the only difference that the customer provides tasks with
        solutions. {EXPERT_FILL_EVAL}</p>

    <p>
        The rules for labeling of <em>objects</em> (solutions) will change slightly. The solution is
        <em>incorrect</em> if:
    </p>

    <ul>
        <li>The task is incorrect (see the criteria and corresponding actions above)</li>
        <li>
            The solution is incorrect – it does not comply with the instructions, choose <code>{WRONG}</code> in
            <code>OK</code> field and specify the reason in <code>COMMENT</code>.
        </li>
    </ul>
    <h2>Labeling quality verification</h2>

    <p>
        In the Toloka task feed, you will see a card with the suffix <em>({SOLUTION_LABELING})</em> in the title and the note <em>{LABELING_QUALITY_VERIFICATION}</em>:
    </p>

    <div>
        <input id="hd-34" class="hide" type="checkbox">
        <label for="hd-34"><strong>Task card example</strong></label>
        <div>
            <table>
                <tbody>
                    <tr>
                        <td style="text-align:center"><h2>Browser</h2></td>
                        <td style="text-align:center"><h2>Mobile app</h2></td>
                    </tr>
                    <tr>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_verification_{ANNOTATION_URL_SUFFIX}browser.png"
                                 alt="task-card-browser" style="width:100%;max-width:500px"/>
                        </td>
                        <td>
                            <img src="https://storage.yandexcloud.net/crowdom-public/instructions/experts/en/task-card_verification_{ANNOTATION_URL_SUFFIX}app.png"
                                 alt="task-card-app" style="width:100%;max-width:500px"/>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <p>The scenario is equivalent to the previous one. In this scenario tasks were solved by workers. You should only evaluate the correctness of the <strong>solution</strong></p>

    <p>
        The solution is <em>incorrect</em> if it does not comply with the instructions, choose <code>{WRONG}</code>
            in <code>OK</code> field and specify the reason in <code>COMMENT</code>.
    </p>
</div>
""",
    }
)

worker_header = base.LocalizedString(
    {
        'RU': '<h1>Инструкция для исполнителей</h1>',
        'EN': '<h1>Instructions for workers</h1>',
    }
)

expert_cut = base.LocalizedString(
    {
        'RU': """
<input id="hd-101" class="hide" type="checkbox">
<label for="hd-101"><strong>Экспертная разметка</strong></label>
""",
        'EN': """
<input id="hd-101" class="hide" type="checkbox">
<label for="hd-101"><strong>Expert labeling</strong></label>
""",
    }
)


def get_formatted_expert_instruction(task_function: base.TaskFunction, lang: str, has_evaluation: bool):
    is_annotation = isinstance(task_function, base.AnnotationFunction)
    return experts[lang].format(
        EXPERT_LABELING_OBJECT_COUNT=2 if not has_evaluation else 3,
        EXPERT_EVALUATION=expert_evaluation[lang] if has_evaluation else '',
        EXPERT_SOLVE_TASK=(expert_solve_task if not is_annotation else expert_solve_annotation_task)[lang],
        EXTRA_COMMENT_REASON=expert_extra_comment_reason[lang] if is_annotation else '',
        EXTRA_SUFFIX=f'{project.CHECK[lang]} ' if is_annotation else '',
        EXPERT_FILL_EVAL=expert_fill_eval[lang] if has_evaluation else '',
        TASK_LABELING=task_labeling[lang],
        SOLUTION_LABELING=solution_labeling[lang],
        TASK_VERIFICATION=task_verification[lang],
        LABELING_QUALITY_VERIFICATION=labeling_quality_verification[lang],
        TASK_NAME=audio_transcript_task[lang] if is_annotation else image_classification_task[lang],
        ANNOTATION_URL_SUFFIX='annotation_' if is_annotation else '',
        WRONG=wrong[lang],
        CORRECT=correct[lang],
    )
