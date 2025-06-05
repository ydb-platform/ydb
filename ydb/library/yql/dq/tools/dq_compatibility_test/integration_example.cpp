// Пример интеграции сериализации DqTask в существующий код YDB
// Этот файл показывает, как сохранить задачу из вашего кода

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <util/stream/file.h>

// Функция для сохранения DqTask в файл
bool SaveDqTaskToFile(const NYql::NDqProto::TDqTask& task, const TString& filename) {
    try {
        TString serializedData;
        if (!task.SerializeToString(&serializedData)) {
            return false;
        }
        
        TFileOutput output(filename);
        output.Write(serializedData);
        output.Finish();
        return true;
    } catch (...) {
        return false;
    }
}

// Пример использования в вашем коде:
void ExampleUsage() {
    // Предположим, у вас есть готовая задача
    NYql::NDqProto::TDqTask task;
    
    // ... настройка задачи ...
    
    // Сохранение для тестирования совместимости
    TString filename = "task_v" + ToString(GetCurrentVersion()) + "_" + ToString(task.GetId()) + ".bin";
    if (SaveDqTaskToFile(task, filename)) {
        // Задача сохранена, можно тестировать на другой версии
    }
}

// Если у вас есть TDqTaskSettings, можно получить из него протобуф:
void SaveFromTaskSettings(const NYql::NDq::TDqTaskSettings& taskSettings, const TString& filename) {
    const auto& protoTask = taskSettings.GetSerializedTask();
    SaveDqTaskToFile(protoTask, filename);
} 